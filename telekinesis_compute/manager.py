import os
import importlib
import time
import json
import asyncio
import telekinesis as tk
import docker
import logging
from functools import partial

# from telekinesis_data import FileSync

def prepare_python_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_python")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies]

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    script = importlib.resources.read_text(__package__, "script_base.py")

    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pyselenium_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pyselenium")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies] + ['selenium']

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    script = importlib.resources.read_text(__package__, "script_base.py")
    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pyvnc_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pyvnc")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies] 

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    script = importlib.resources.read_text(__package__, "script_base.py")
    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_pytorch_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, f"Dockerfile_pytorch")
    deps_pip_names = [d if isinstance(d, str) else d[0] for d in dependencies]
    deps_import_names = [d if isinstance(d, str) else d[1] for d in dependencies] + ['torch']

    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in deps_pip_names))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    script = importlib.resources.read_text(__package__, "script_base.py")

    with open(os.path.join(path, 'script.py'), 'w') as file_out:
        file_out.write(script)

def prepare_js_files(path, dependencies):
    dockerbase = importlib.resources.read_text(__package__, "Dockerfile_js")
    dockerfile = dockerbase.replace('{{PKG_DEPENDENCIES}}', '\n'.join('RUN npm install '+ d for d in dependencies))

    with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
        file_out.write(dockerfile)

    script = importlib.resources.read_text(__package__, "script_base.js")

    with open(os.path.join(path, 'script.js'), 'w') as file_out:
        file_out.write(script)

class AppManager:
    def __init__(self, session, path, sudo_rm=False):
        self.running = {}
        self.stop_callback = None
        self.client = docker.from_env()
        self.url = list(session.connections)[0].url
        self._sudo = sudo_rm
        self._session = session
        self._logger = logging.getLogger(__name__)
        self.path = os.path.abspath(path)
        self.tasks = {'delayed_provisioning': {}, 'stop_callback': {}, 'check_running_loop': asyncio.create_task(self.loop_check_running())}

    async def build_image(self, pkg_dependencies, base):
        tag = '-'.join(['tk', base, *[d.replace('@', 'at/').lower() if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        if base == 'python':
            prepare_python_files(self.path, pkg_dependencies)
        elif base == 'pyselenium':
            prepare_pyselenium_files(self.path, pkg_dependencies)
        elif base == 'pyvnc':
            prepare_pyvnc_files(self.path, pkg_dependencies)
        elif base == 'pytorch':
            prepare_pytorch_files(self.path, pkg_dependencies)
        elif base == 'js':
            prepare_js_files(self.path, pkg_dependencies)
        else:
            raise NotImplementedError("Only implemented bases are 'python', 'pyselenium', 'pytorch', 'pyvnc', and 'js'")


        cmd = f'docker build -t {tag} {self.path}'

        build = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE
        )
        await build.stdout.read()

    def set_stop_callback(self, stop_callback):
        self.stop_callback = stop_callback

    async def get_pod(self, pkg_dependencies, base, cpus, memory, gpu, upgrade=False):
        # tag = '-'.join(['tk', base, *[d if isinstance(d, str) else d[0] for d in pkg_dependencies]])
        tag = '-'.join(['tk', base, *[d.replace('@', 'at/').lower() if isinstance(d, str) else d[0] for d in pkg_dependencies]])

        client_session = tk.Session()
        client_pubkey = client_session.session_key.public_serial()

        # data_path = os.path.join(self.path, client_pubkey[:32].replace('/','-'))

        pod_wrapper = PodWrapper(client_pubkey, self, base, cpus, memory, gpu)

        def create_callbackable():
            e = asyncio.Event()
            data = {}

            async def awaiter():
                await e.wait()
                self._logger.info('pod %s: called awaiter', client_pubkey[:6])
                return data['data']

            def callback(*x):
                data['data'] = x
                e.set()
                return pod_wrapper.stop

            return awaiter, callback

        awaiter, callback = create_callbackable()

        if upgrade or not self.client.images.list(name=tag):
            await self.build_image(pkg_dependencies, base)
        
        # os.mkdir(data_path)

        route = await tk.Telekinesis(callback, self._session)._delegate(client_pubkey)

        _key_dump = json.dumps(client_session.session_key._private_serial().decode().strip('\n'))
        environment = [
            f"TELEKINESIS_URL='{self.url}'",
            f"TELEKINESIS_POD_NAME='id={client_session.session_key.public_serial()[:6]}, base={base}, cpus={cpus:.2f}, memory={int(memory)}, gpu={gpu}'",
            f"TELEKINESIS_ROUTE_STR='{json.dumps(route.to_dict())}'",
            "TELEKINESIS_PRIVATE_KEY_STR='"+_key_dump+"'"
        ]

        cmd = " ".join([
            f"docker run -e {' -e '.join(environment)} -d --network=host ",
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

        pod_wrapper._set_container(container_id, pod, update_callbacks)
        self.running[pod_wrapper.id] = pod_wrapper
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

    async def check_running(self):
        running_containers = (await (await asyncio.create_subprocess_shell(
            'docker container ls -q --no-trunc', 
            stdout=asyncio.subprocess.PIPE)
        ).stdout.read()).decode().strip('\n').split('\n')

        for pod_wrapper in self.running.values():
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
    def __init__(self, pod_id, manager, base, cpus, memory, gpu):
        self._logger = logging.getLogger(__name__)
        self._sudo = manager._sudo
        self._manager = manager
        self.container_id = None
        self.pod_update_callbacks = None
        self.pod = None
        # self.filesync = None
        self.id = pod_id
        self.base = base
        self.cpus = cpus
        self.memory = memory
        self.gpu = gpu
        self.idle_timeout = None
        self.idle_stop_time = None
        self.run_timeout = None
        self.run_stop_time = None
        self.stop_task = None
        # self.bind_dir = bind_dir
        self.stopping = False

    def reset_timeout(self):
        self._logger.info('pod %s: keep alive', self.id[:6])
        if self.idle_timeout is not None or self.run_timeout is not None:
            if self.idle_timeout is not None:
                self.idle_stop_time = time.time() + self.idle_timeout
            if self.run_timeout is not None:
                self.run_stop_time = time.time() + self.run_timeout
            if self.stop_task is None:
                self.stop_task = asyncio.create_task(self.delayed_stop())

    async def delayed_stop(self):
        stop_time = min(self.run_stop_time or 10**11, self.idle_stop_time or 10**1)
        print('waiting for stop_time', stop_time-time.time())
        await asyncio.sleep(max(2, stop_time-time.time()))
        if self.run_stop_time and self.run_stop_time < time.time():
            await self.stop()
            return

        elif self.idle_stop_time and self.idle_stop_time < time.time():
            p = await asyncio.create_subprocess_shell(
                'docker stats --no-stream --format "{{.CPUPerc}}" '+self.container_id[:10],
                stdout=asyncio.subprocess.PIPE)
            cpu_utilization = float((await p.stdout.read()).decode().strip('%\n'))
            if cpu_utilization < 1: # 1%
                await self.stop()
                return
            else:
                self._logger.info('pod %s: extending because of cpu_utilization %s', self.id[:6], cpu_utilization)
                self.idle_stop_time = time.time() + self.idle_timeout

        # print('extending', self.autostop_time - time.time())
        self.stop_task = asyncio.create_task(self.delayed_stop())
         
    async def update_params(self, idle_timeout, run_timeout, name=None):
        self.idle_timeout = idle_timeout
        self.run_timeout = run_timeout

        self.reset_timeout()

        new_name = name and f'id={self.id[:6]}, base={self.base}, cpus={self.cpus:.2f}, memory={int(self.memory)}, gpu={self.gpu}, name={name}'

        self.pod = await self.pod_update_callbacks(self.reset_timeout or 0, 0, new_name)

        return self.pod
    
    async def stop(self):
        logs = await (await asyncio.create_subprocess_shell(f'docker logs {self.container_id}', stdout=asyncio.subprocess.PIPE)).stdout.read()

        if not self.stopping:
            self.stopping = True
            try:
                await self.pod.stop()._timeout(5)
            except asyncio.TimeoutError:
                await asyncio.create_subprocess_shell(f'docker container stop -t 0 {self.container_id}')
        
        if self.id in self._manager.running:
            self._manager.running.pop(self.id, None)
            
            status = await (await asyncio.create_subprocess_shell(
                f'docker container ls --all -f id={self.container_id} --format '+"{{.Status}}", 
                stdout=asyncio.subprocess.PIPE)).stdout.read()

            logs = logs.decode() + '\n\n' + status.decode()

            if self._manager.stop_callback:
                await self._manager.stop_callback(self.id, logs)

            # if self.filesync and self.filesync.task:
            #     self.filesync.task.cancel()
            #     await self.filesync.sync(False)
            #     proc = await asyncio.create_subprocess_shell(
            #         f'{"sudo " if self._sudo else ""}rm -rf {self.filesync.support_dir}',
            #         stderr=asyncio.subprocess.PIPE, 
            #         stdout=asyncio.subprocess.PIPE)
            #     await proc.stderr.read()
            #     await proc.stdout.read()
            
            # proc = await asyncio.create_subprocess_shell(
            #     f'{"sudo " if self._sudo else ""}rm -rf {self.bind_dir}', 
            #     stderr=asyncio.subprocess.PIPE, 
            #     stdout=asyncio.subprocess.PIPE)
            # await proc.stderr.read()
            # await proc.stdout.read()

            await asyncio.create_subprocess_shell(f'docker container rm -f {self.container_id}', )

    # def bind_data(self, data):
    #     bind_path = os.path.join(self.path, self.id[:32].replace('/','-'))
    #     data_path = os.path.join(bind_path, 'synced')
    #     support_path = bind_path +'_support'
    #     os.mkdir(support_path)
    #     os.mkdir(data_path)

        # self.filesync = FileSync(data, data_path, support_path)

    def _set_container(self, container_id, pod, update_callbacks):
        self.container_id = container_id
        self.pod = pod
        self.pod_update_callbacks = update_callbacks
