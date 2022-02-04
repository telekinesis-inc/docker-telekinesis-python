import os
import importlib
import time
import json
import asyncio
import telekinesis as tk
import docker

def prepare_python_files(path, *dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_python")
    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in dependencies))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join(['import '+ d.replace('-', '_') for d in dependencies] + [scriptbase])
    

    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pytorch_files(path, *dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pytorch")
    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in dependencies))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    scriptbase = importlib.resources.read_text(__package__, "script_base.py")
    script = '\n'.join(['import '+ d.replace('-', '_') for d in set(dependencies).union(['torch'])] + [scriptbase])
    

    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_js_files(path, *dependencies):
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
        self.tasks = {}

    async def build_image(self, *dependencies, language='python'):
        tag = '-'.join(['tk', language, *dependencies])
        if language == 'python':
            prepare_python_files(self.path, *dependencies)
        elif language == 'pytorch':
            prepare_pytorch_files(self.path, *dependencies)
        elif language == 'js':
            prepare_js_files(self.path, *dependencies)
        else:
            raise NotImplementedError("Only implemented languages are 'python' and 'js'")

        
        cmd = f'docker build -t {tag} {self.path}'
        
        build = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE
        )
        await build.stdout.read()
        # await self.client.images.build(path_dockerfile='./docker_telekinesis_python/', tag=tag)

    async def create_container(self, *dependencies, language='python', **kwargs):
        tag = '-'.join(['tk', language, *dependencies])


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

        environment=[
            "TELEKINESIS_URL='"+self.url+"'",
            "TELEKINESIS_POD_NAME='"+tag+"'",
            "TELEKINESIS_ROUTE_STR='"+json.dumps(route.to_dict())+"'",
            "TELEKINESIS_PRIVATE_KEY_STR='"+json.dumps(client_session.session_key._private_serial().decode().strip('\n'))+"'"]
        
        cmd = f"docker run -e {' -e '.join(environment)} -d --rm --network=host {'--gpus all --ipc=host' if language == 'pytorch' else ''} {' '.join('--'+key+'='+val for key, val in kwargs.items())} -l telekinesis-compute {tag}"
        
        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE
        )

        container_id = (await process.stdout.read()).decode().replace('\n','')

        pod_name, pod = await awaiter()
        assert pod_name == tag
        # container = self.client.containers.get(container_id)
        
        # d.update({'container_id': container_id})
        
        return pod

    async def clear_containers(self):
        [c.stop(timeout=0) for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]
        [c.remove() for c in self.client.containers.list(all=True, filters={'label': 'telekinesis-compute'})]

        return self.client.images.prune()
    
    async def get_pod(self, name, *imports, upgrade=False, language='python', **kwargs):
        tag = '-'.join(['tk', language, *imports])
        if not self.ready.get(tag):
            print('awaiting provisioning')
            await self.provision(1, *imports, language=language, upgrade=upgrade, **kwargs)
        d = self.ready[tag].pop()
        # async def delayed_provisioning(t):
        #     await asyncio.sleep(1)
        #     await self.provision(1, language, *imports, upgrade=upgrade, **kwargs)
        #     self.tasks.pop(t)
            
        t = time.time()
        # self.tasks[t] = asyncio.create_task(delayed_provisioning(t))
        self.running[name] = [*(self.running.get(name) or []), d]
        return d

    async def provision(self, number, *imports, language='python', upgrade=False, **kwargs):
        print('provisioning', number)
        tag = '-'.join(['tk', language, *imports])
        if not tag in self.ready:
            self.ready[tag] = []

        if upgrade or not self.client.images.list(name=tag):
            await self.build_image(*imports, language=language)

        self.ready[tag].extend(
            await asyncio.gather(*[self.create_container(*imports, language=language **kwargs) for _ in range(number)])
        )
    