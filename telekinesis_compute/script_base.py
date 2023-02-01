import sys
import asyncio
import os
import signal
import json
import time
from contextlib import redirect_stdout, redirect_stderr
from collections import deque
import threading

import telekinesis as tk


class StdOutCapture:
    def __init__(self, callback, loop=None, direct=False):
        self.callback = callback
        self.loop = loop
        self.direct = direct
        self.output_accumulator = []

    def write(self, out):
        if out != "\n" and not self.direct:
            self.output_accumulator.append(out)
        else:
            if self.direct:
                self.output_accumulator.append(out)
            self.flush()
    def flush(self):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.callback(*self.output_accumulator), self.loop)
        else:
            asyncio.create_task(self.callback(*self.output_accumulator))
        self.output_accumulator.clear()
    def isatty(self):
        return False

class PrintCB:
    def __init__(self, print_callback, call_data):
        self.print_callback = print_callback
        self.call_data = call_data
    async def __call__(self, *args):
        self.call_data['log'][time.time()] = args
        if self.print_callback:
            await self.print_callback(*args)


class Pod:
    def __init__(self, name, executor, lock):
        self.name = name
        self.calls = []
        self.executor = executor
        self._lock = lock
        self._stop_callback = None
        self._keep_alive_callback = None

    async def execute(self, code, inputs=None, print_callback=None, secret=False):
        return await self._execute('code', code, inputs, print_callback, secret)

    async def stop(self):
        async def cleanup():
            self.executor.stop_lock.set()
            if self._stop_callback:
                await self._stop_callback()
            self._lock.set()
            self.interrupt()
        asyncio.create_task(cleanup())

    def set_concurrency(self, concurrent=True):
        """concurrent: bool"""
        self.executor.concurrent = concurrent

    def interrupt(self):
        self.executor.queue.clear()
        if self.executor.call_lock.is_set():
            os.kill(os.getpid(), signal.SIGINT)

    async def execute_command(self, command, print_callback=None, secret=False):
        return await self._execute('command', command, print_callback and True, print_callback, secret)

    async def install_package(self, package_name, print_callback=None):
        return await self.execute_command('pip install '+ package_name, print_callback)
    
    def write_file(self, path, data):
        if isinstance(data, str):
            with open(path, 'w') as f:
                f.write(data)
        elif isinstance(data, bytes):
            with open(path, 'wb') as f:
                f.write(data)
        else:
            raise TypeError('data must be of type "str" or "bytes", was "' + type(data).__name__ + '" instead' )
        
        return os.path.getsize(path)

    async def _execute(self, job_type, code, inputs=None, print_callback=None, secret=False):
        lock = asyncio.Event()
        timestamp = time.time()
        if job_type == 'code':
            call_data = {
                'code': '<HIDDEN>' if secret else code, 
                'inputs': '<HIDDEN>' if secret else inputs, 
                'status': 'RUNNING', 'log': {}}
        elif job_type == 'command':
            call_data = {
                'command': '<HIDDEN>' if secret else code, 
                'status': 'RUNNING', 'log': {}}
        else:
            raise f'job_type: {job_type} should be either code or command'

        self.calls.append((timestamp, call_data)) 

        inputs = inputs or {}

        async def lock_set():
            lock.set()

        job = Job(job_type, code, inputs, PrintCB(print_callback, call_data), lock_set, asyncio.get_event_loop())
        await self.executor.enqueue(job)
        await lock.wait()
        t, out = job.returns
        call_data['output'] = out
        if t == 'return':
            call_data['status'] = 'SUCCEEDED'
            return out
        else:
            call_data['status'] = 'ERROR'
            raise out

    def _update_callbacks(self, keep_alive_callback, runner, name=None):
        self._keep_alive_callback = keep_alive_callback
        if name:
            self.name = name
        # self._runner = runner
        return self

    def _keep_alive(self, metadata):
        if self._keep_alive_callback and metadata.caller.session[0] != self._keep_alive_callback._target.session[0]:
            asyncio.create_task(self._keep_alive_callback()._execute())

    def __repr__(self):
        return f'Pod({self.name})'


class Job:
    def __init__(self, job_type, code, inputs, print_callback, cb, loop):
        self.type = job_type
        self.code = code
        self.inputs =  inputs
        self.print_callback = print_callback
        self.cb = cb
        self.loop = loop
        self.returns = None


class Executor:
    def __init__(self):
        self.call_lock = None
        self.stop_lock = threading.Event()
        self.queue = deque()
        self.concurrent = True
        self.running = {}

    async def enqueue(self, job):
        self.queue.append(job)
        self.call_lock.set()

    async def _execute_command(self, command, stream_output, print_callback, loop):
        async def _read_stream(stream, cb):  
            while True:
                line = await stream.readline()
                if line:
                    cb(line.decode().rstrip('\n'))
                else:
                    break

        async def _stream_subprocess(cmd, stdout_cb, stderr_cb):  
            process = await asyncio.create_subprocess_shell(cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)

            await asyncio.wait([
                asyncio.create_task(_read_stream(process.stdout, stdout_cb)),
                asyncio.create_task(_read_stream(process.stderr, stderr_cb))
            ])
            return await process.wait()

        if stream_output:
            stderr = StdOutCapture(print_callback, loop, True)
            with redirect_stderr(stderr):
                stdout = StdOutCapture(print_callback, loop)
                with redirect_stdout(stdout):
                    await _stream_subprocess(command, print, lambda *x: print(*x, file=sys.stderr))
        else:
            process = await asyncio.create_subprocess_shell(command,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            return (await process.stdout.read()).decode().split('\n')

    async def _execute_code(self, code, inputs, print_callback, loop):
        prefix = f'async def _tkc_wrapper({", ".join([k for k in inputs])}):\n'
        content = ('\n'+code).replace('\n', '\n ')
        # suffix = "\n for _var in dir():\n  if _var[0] != '_':\n   _tkc_new_vars[_var] = eval(_var)"

        tmp = {}
        exec(prefix+content, tmp)
        if print_callback:
            stderr = StdOutCapture(print_callback, loop, True)
            with redirect_stderr(stderr):
                stdout = StdOutCapture(print_callback, loop)
                with redirect_stdout(stdout):
                    out = await tmp['_tkc_wrapper'](**inputs)
        else:
            out = await tmp['_tkc_wrapper'](**inputs)
        return out

    async def run(self):
        while not self.stop_lock.is_set():
            while not self.queue:
                if self.stop_lock.is_set(): return
                if self.running:
                    self.call_lock = asyncio.Event()
                    try:
                        await asyncio.wait_for(self.call_lock.wait(), .1)
                    except asyncio.TimeoutError:
                        pass
                else:
                    self.call_lock = threading.Event()
                    self.call_lock.wait(3)

            job = self.queue.popleft()
            print('>>>>>>', file=sys.stderr)
            if self.concurrent:
                self.running[id(job)] = asyncio.create_task(self._handle_job(job))
                # print(t, file=sys.stderr)
            else:
                await self._handle_job(job)
    
    async def _handle_job(self, job):
        print('handle_job', file=sys.stderr)
        try:
            if job.type == 'code':
                r = await self._execute_code(job.code, job.inputs, job.print_callback, job.loop)
            else:
                r = await self._execute_command(job.code, job.inputs, job.print_callback, job.loop)
            t = 'return'
        except (KeyboardInterrupt, Exception) as e:
            r = e
            t = 'raise'
        self.future = None
        job.returns = t, r
        self.running.pop(id(job), None)
        asyncio.run_coroutine_threadsafe(job.cb(), job.loop)


def decode_args():
    kwargs = {}
    for env, val in os.environ.items():
        if env.startswith('TELEKINESIS_'):
            key = env[len('TELEKINESIS_'):].lower()
            kwargs[key] = val
    args_order = ('url', 'pod_name', 'private_key_str', 'key_password', 'key_filename', 'route_str')
    argv = sys.argv[1:]
    key = None
    in_kws = False
    for i, arg in enumerate(argv):
        if key:
            if arg.startswith('--'):
                kwargs[key] = True
            else:
                kwargs[key] = arg
                key = None
                continue
        if arg.startswith('--'):
            in_kws = True
            key = arg[2:]
            if i == (len(argv)-1):
                kwargs[key] = True
            continue
        if in_kws:
            raise SyntaxError(f'Error parsing arguments: {sys.argv}')
        kwargs[args_order[i]] = arg

    return kwargs


async def start_pod(executor, url, pod_name, private_key_str=None, key_password=None, key_filename=None, route_str=None, **_):
    private_key = None
    if private_key_str:
        private_key = tk.cryptography.PrivateKey.from_private_serial(json.loads(private_key_str).encode(), key_password)
        if key_filename:
            private_key.save_key_file(key_filename, key_password)
    elif key_filename:
        private_key = tk.cryptography.PrivateKey(key_filename, key_password)

    lock = asyncio.Event()
    pod = Pod(pod_name, executor, lock)

    if route_str:
        entrypoint = await tk.Entrypoint(url, private_key)
        route = tk.Route(**json.loads(route_str))
        pod._stop_callback = await tk.Telekinesis(route, entrypoint._session)(pod._update_callbacks, pod)
        entrypoint._session.message_listener = pod._keep_alive
    else:
        await tk.authenticate(url, private_key).data.put(pod, pod_name)
    
    await lock.wait()


def run_in_new_event_loop(future):
    l = asyncio.new_event_loop()
    asyncio.set_event_loop(l)

    l.run_until_complete(future)


if __name__ == '__main__':
    executor = Executor()

    threading.Thread(target=run_in_new_event_loop, args=[start_pod(executor, **decode_args())]).start()

    asyncio.run(executor.run())
