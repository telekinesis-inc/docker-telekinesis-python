import os
import importlib
import time
import json
import asyncio
import telekinesis as tk
import docker
from functools import partial

def prepare_python_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_python")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies]

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join(['import '+ d.replace('-', '_') for d in deps_import_names] + [scriptbase])


    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pytorch_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pytorch")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies]

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join(['import '+ d.replace('-', '_') for d in set(deps_import_names).union(['torch'])] + [scriptbase])


    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_js_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, "Dockerfile_js")
    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN npm install '+ d for d in dependencies))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.js")
    script = '\n'.join([f'require({d});' for d in dependencies] + [scriptbase])

    with open(os.path.join(path, 'script.js'), 'w') as file_out:
        file_out.write(script)

class AppManager:
    def __init__(self, session, path, url=None):
        self.running = {}
        self.ready = {}
        self.client = docker.from_env()
        self.url = url or list(session.connections)[0].url
        self._session = session
        self.path = path
        self.tasks = {'delayed_provisioning': {}, 'stop_callback': {}}

    async def build_image(self, pkg_dependencies, base):
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if base == 'python':
            prepare_python_files(self.path, pkg_dependencies)
        elif base == 'pytorch':
            prepare_pytorch_files(self.path, pkg_dependencies)
        elif base == 'js':
            prepare_js_files(self.path, pkg_dependencies)
        else:
            raise NotImplementedError("Only implemented bases are 'python', 'pytorch' and 'js'")


        cmd = f'docker build -t {tag} {self.path}'

        build = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE
        )
        await build.stdout.read()
        # await self.client.images.build(path_dockerfile='./docker_telekinesis_python/', tag=tag)

    async def start_container(self, pkg_dependencies, base, cpus, memory, gpu):
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])


        def create_callbackable():
            e = asyncio.Event()
            data = {}

            async def awaiter():
                await e.wait()
                print('called awaiter')
                return data['data']

            return (awaiter, lambda *x: data.update({'data': x}) or e.set())

        awaiter, callback = create_callbackable()

        client_session = tk.Session()

        route = await tk.Telekinesis(callback, self._session)._delegate(client_session.session_key.public_serial())

        _key_dump = json.dumps(client_session.session_key._private_serial().decode().strip('\n'))
        environment = [
            f"TELEKINESIS_URL='{self.url}'",
            f"TELEKINESIS_POD_NAME='id={client_session.session_key.public_serial()[:6]}, base={base}, cpus={cpus:.2f}, memory={int(memory)}, gpu={gpu}'",
            f"TELEKINESIS_ROUTE_STR='{json.dumps(route.to_dict())}'",
            "TELEKINESIS_PRIVATE_KEY_STR='"+_key_dump+"'"
        ]

        cmd = " ".join([
            f"docker run -e {' -e '.join(environment)} -d --rm --network=host",
            f"{'--gpus all --ipc=host' if gpu else ''} --cpus={cpus:.2f} --memory='{int(memory)}m'",
            f"-l telekinesis-compute {tag}"
        ])

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE
        )

        container_id = (await process.stdout.read()).decode().replace('\n','')

        update_stop_callback, pod = await awaiter()
        # container = self.client.containers.get(container_id)

        pod_wrapper = PodWrapper(pod, update_stop_callback)
        return pod_wrapper

    async def clear_containers(self):
        [c.stop(timeout=0) for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]
        [c.remove() for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]

        return self.client.images.prune()

    async def get_pod(
        self, pkg_dependencies, account_id, base='python', cpus=1.0, memory=2000, gpu=False, autostop_timeout=None, stop_callback=None, 
        provision=False, upgrade=False
    ):
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if not self.ready.get(tag):
            print('awaiting provisioning')
            await self.provision(1, pkg_dependencies, base, cpus, memory, gpu, upgrade)
        pod_wrapper = self.ready[tag].pop()

        self.running[account_id] = {**self.running.get(account_id, {}), pod_wrapper.id: pod_wrapper}

        if stop_callback:
            await pod_wrapper.update_stop_callback(partial(stop_callback, pod_wrapper.id))
        
        if autostop_timeout:
            ...

        if provision:
            t = time.time()
            async def delayed_provisioning(t):
                await asyncio.sleep(1)
                await self.provision(1, base, pkg_dependencies, base, cpus, memory, gpu, upgrade)
                self.tasks['delayed_provisioning'].pop(t)
            self.tasks['delayed_provisioning'][t] = asyncio.create_task(delayed_provisioning(t))

        return pod_wrapper.pod

    async def provision(self, number, pkg_dependencies, base, cpus, memory, gpu, upgrade):
        print('provisioning', number)
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if not tag in self.ready:
            self.ready[tag] = []

        if upgrade or not self.client.images.list(name=tag):
            await self.build_image(pkg_dependencies, base)

        self.ready[tag].extend(
            await asyncio.gather(*[self.start_container(pkg_dependencies, base, cpus, memory, gpu) for _ in range(number)])
        )

    async def stop(self, account_id, pod_id, callback=None):
        self.running.get(account_id, {}).pop(pod_id)
        if callback:
            self.tasks['stop_callback'][time.time()] = asyncio.create_task(callback(pod_id)._execute())

class PodWrapper:
    def __init__(self, pod, update_stop_callback):
        self.update_stop_callback = update_stop_callback
        self.pod = pod
        self.id = pod._target.session[0]

