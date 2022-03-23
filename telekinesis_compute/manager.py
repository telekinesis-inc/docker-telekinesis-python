import os
import importlib
import time
import json
import asyncio
import telekinesis as tk
import docker
import logging
from functools import partial

from telekinesis_data import FileSync

def prepare_python_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_python")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies]

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join([
        'import sys',
        *[
            f'try: import {d.replace("-", "_")}\nexcept Exception as e: print(e, file=sys.stderr)'
            for d in deps_import_names
        ],
        scriptbase
    ])


    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pytorch_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pytorch")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies] + ['torch']

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join([
        'import sys',
        *[
            f'try: import {d.replace("-", "_")}\nexcept Exception as e: print(e, file=sys.stderr)'
            for d in deps_import_names
        ],
        scriptbase
    ])

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
    def __init__(self, session, path, sudo_rm=False):
        self.running = {}
        self.ready = {}
        self.client = docker.from_env()
        self.url = list(session.connections)[0].url
        self._sudo = sudo_rm
        self._session = session
        self._logger = logging.getLogger(__name__)
        self.path = os.path.abspath(path)
        self.tasks = {'delayed_provisioning': {}, 'stop_callback': {}, 'check_running_loop': asyncio.create_task(self.loop_check_running())}

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

        client_session = tk.Session()
        client_pubkey = client_session.session_key.public_serial()

        def create_callbackable():
            e = asyncio.Event()
            data = {}

            async def awaiter():
                await e.wait()
                self._logger.info('pod %s: called awaiter', client_pubkey[:6])
                return data['data']

            return (awaiter, lambda *x: data.update({'data': x}) or e.set())

        awaiter, callback = create_callbackable()

        
        data_path = os.path.join(self.path, client_pubkey[:32].replace('/','-'))
        os.mkdir(data_path)

        route = await tk.Telekinesis(callback, self._session)._delegate(client_pubkey)

        _key_dump = json.dumps(client_session.session_key._private_serial().decode().strip('\n'))
        environment = [
            f"TELEKINESIS_URL='{self.url}'",
            f"TELEKINESIS_POD_NAME='id={client_session.session_key.public_serial()[:6]}, base={base}, cpus={cpus:.2f}, memory={int(memory)}, gpu={gpu}'",
            f"TELEKINESIS_ROUTE_STR='{json.dumps(route.to_dict())}'",
            "TELEKINESIS_PRIVATE_KEY_STR='"+_key_dump+"'"
        ]

        cmd = " ".join([
            f"docker run -e {' -e '.join(environment)} -d --network=host -v {data_path}:/home/user/data/",
            f"{'--gpus all --ipc=host' if gpu else ''} --cpus={cpus:.2f} --memory='{int(memory)}m'",
            f"-l telekinesis-compute {tag}"
        ])

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        error = (await process.stderr.read()).decode()

        if error:
            self._logger.error('pod %s: error starting - %s', client_pubkey[:6], error)
            raise Exception(f'error starting pod: {error}')

        container_id = (await process.stdout.read()).decode().replace('\n','')

        update_callbacks, pod = await awaiter()
        # container = self.client.containers.get(container_id)

        pod_wrapper = PodWrapper(container_id, pod, update_callbacks, data_path, self._sudo)
        return pod_wrapper

    async def clear_containers(self, clear_path=False):
        [c.stop(timeout=0) for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]
        [c.remove() for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]
        
        if clear_path:
            proc = await asyncio.create_subprocess_shell(
                f'{"sudo " if self._sudo else ""}rm -rf {self.path}', 
                stderr=asyncio.subprocess.PIPE, 
                stdout=asyncio.subprocess.PIPE)
            await proc.stderr.read()
            await proc.stdout.read()

        if not os.path.exists(self.path):
            os.mkdir(self.path)

        return self.client.images.prune()

    async def get_pod(
        self, pkg_dependencies, account_id, base='python', cpus=1.0, memory=2000, gpu=False, autostop_timeout=None, bind_data=None, stop_callback=None, 
        provision=False, upgrade=False
    ):
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if not self.ready.get((tag, int(cpus*1000), int(memory))):
            self._logger.info('awaiting provisioning')
            await self.provision(1, pkg_dependencies, base, cpus, memory, gpu, upgrade)
        pod_wrapper = self.ready[(tag, int(cpus*1000), int(memory))].pop()

        self.running[account_id] = {**self.running.get(account_id, {}), pod_wrapper.id: pod_wrapper}

        # if stop_callback or autostop_timeout is not None:
        await pod_wrapper.update_params(partial(self.stop, account_id, pod_wrapper.id, stop_callback), autostop_timeout)
        pod_wrapper.reset_timeout()
        
        if bind_data:
            bind_path = os.path.join(self.path, pod_wrapper.id[:32].replace('/','-'))
            data_path = os.path.join(bind_path, 'synced')
            support_path = bind_path +'_support'
            os.mkdir(support_path)
            os.mkdir(data_path)

            pod_wrapper.filesync = FileSync(bind_data, data_path, support_path)

        if provision:
            t = time.time()
            async def delayed_provisioning(t):
                await asyncio.sleep(1)
                await self.provision(1, base, pkg_dependencies, base, cpus, memory, gpu, upgrade)
                self.tasks['delayed_provisioning'].pop(t)
            self.tasks['delayed_provisioning'][t] = asyncio.create_task(delayed_provisioning(t))

        return pod_wrapper.pod

    async def provision(self, number, pkg_dependencies, base, cpus, memory, gpu, upgrade):
        self._logger.info('provisioning %s pods', number)
        tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if not (tag, int(cpus*1000), int(memory)) in self.ready:
            self.ready[(tag, int(cpus*1000), int(memory))] = []

        if upgrade or not self.client.images.list(name=tag):
            await self.build_image(pkg_dependencies, base)

        self.ready[(tag, int(cpus*1000), int(memory))].extend(
            await asyncio.gather(*[self.start_container(pkg_dependencies, base, cpus, memory, gpu) for _ in range(number)])
        )

    async def stop(self, account_id, pod_id, callback=None, logs=None):
        p = self.running.get(account_id, {}).pop(pod_id)
        if p and callback:
            self.tasks['stop_callback'][time.time()] = asyncio.create_task(callback(pod_id, logs=logs)._execute())
        elif callback:
            self._logger.info('pod %s: not found in manager.running', pod_id[:6])

    async def check_running(self):
        running_containers = (await (await asyncio.create_subprocess_shell(
            'docker container ls -q --no-trunc', 
            stdout=asyncio.subprocess.PIPE)
        ).stdout.read()).decode().strip('\n').split('\n')

        for account_pods in self.running.values():
            for pod_wrapper in account_pods.values():
                if pod_wrapper.container_id not in running_containers:
                    self._logger.info('pod %s: container %s stopped', pod_wrapper.id[:6], pod_wrapper.container_id[:8])
                    await pod_wrapper.stop(False)
    
    async def loop_check_running(self):
        while True:
            # try:
                await asyncio.sleep(15)
                await self.check_running()
            # except BaseException:
                # pass

class PodWrapper:
    def __init__(self, container_id, pod, update_callbacks, bind_dir, sudo_rm=False):
        self._logger = logging.getLogger(__name__)
        self._sudo = sudo_rm
        self.container_id = container_id
        self.pod_update_callbacks = update_callbacks
        self.pod = pod
        self.id = pod._target.session[0]
        self.stop_callback = None
        self.autostop_timeout = None
        self.autostop_time = 0
        self.autostop_task = None
        self.filesync = None
        self.bind_dir = bind_dir

    def reset_timeout(self):
        self._logger.info('pod %s: keep alive', self.id[:6])
        if self.autostop_timeout is not None:
            self.autostop_time = time.time() + self.autostop_timeout
            if self.autostop_task is None:
                self.autostop_task = asyncio.create_task(self.autostop(self.autostop_timeout))

    async def autostop(self, delay):
        await asyncio.sleep(delay)
        if self.autostop_time and self.autostop_time < time.time():
            p = await asyncio.create_subprocess_shell(
                'docker stats --no-stream --format "{{.CPUPerc}}" '+self.container_id[:10],
                stdout=asyncio.subprocess.PIPE)
            cpu_utilization = float((await p.stdout.read()).decode().strip('%\n'))
            if cpu_utilization < 1: # 1%
                await self.stop()
            else:
                self._logger.info('pod %s: extending because of cpu_utilization %s', self.id[:6], cpu_utilization)
                self.autostop_task = asyncio.create_task(self.autostop(max(2, self.autostop_timeout)))
        else:
            # print('extending', self.autostop_time - time.time())
            self.autostop_task = asyncio.create_task(self.autostop(max(2, self.autostop_time-time.time())))
         
    async def update_params(self, stop_callback, autostop_timeout):
        self.stop_callback = stop_callback
        self.autostop_timeout = autostop_timeout

        await self.pod_update_callbacks(partial(self.stop, False), self.reset_timeout or 0)
    
    async def stop(self, stop_pod=True):
        logs = await (await asyncio.create_subprocess_shell(f'docker logs {self.container_id}', stdout=asyncio.subprocess.PIPE)).stdout.read()

        if stop_pod:
            try:
                await self.pod.stop()._timeout(5)
            except asyncio.TimeoutError:
                await asyncio.create_subprocess_shell(f'docker container stop -t 0 {self.container_id}')
        
        status = await (await asyncio.create_subprocess_shell(
            f'docker container ls --all -f id={self.container_id} --format '+"{{.Status}}", 
            stdout=asyncio.subprocess.PIPE)).stdout.read()

        logs = logs.decode() + '\n\n' + status.decode()

        if self.stop_callback:
            await self.stop_callback(logs=logs)

        if self.filesync and self.filesync.task:
            self.filesync.task.cancel()
            await self.filesync.sync(False)
            proc = await asyncio.create_subprocess_shell(
                f'{"sudo " if self._sudo else ""}rm -rf {self.filesync.support_dir}',
                stderr=asyncio.subprocess.PIPE, 
                stdout=asyncio.subprocess.PIPE)
            await proc.stderr.read()
            await proc.stdout.read()
        
        proc = await asyncio.create_subprocess_shell(
            f'{"sudo " if self._sudo else ""}rm -rf {self.bind_dir}', 
            stderr=asyncio.subprocess.PIPE, 
            stdout=asyncio.subprocess.PIPE)
        await proc.stderr.read()
        await proc.stdout.read()

        await asyncio.create_subprocess_shell(f'docker container rm -f {self.container_id}', )

        