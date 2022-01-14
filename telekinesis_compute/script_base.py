import sys
import asyncio
import os
import signal
import json
from contextlib import redirect_stdout, redirect_stderr
from collections import deque
import threading

import telekinesis as tk


class StdOutCapture:
    def __init__(self, callback, loop=None, direct=False):
        self.callback = callback
        self.loop = loop
        self.direct = direct
        self.acc = []

    def write(self, out):
        if out != "\n" and not self.direct:
            self.acc.append(out)
        else:
            if self.direct:
                self.acc.append(out)
            self.flush()
    def flush(self):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.callback(*self.acc), self.loop)
        else:
            asyncio.create_task(self.callback(*self.acc))
        self.acc.clear()

class Instance:
    def __init__(self, name, executor, lock):
        self.name = name
        self.scopes = {}
        self.executor = executor
        self.log = []
        self.lock = lock

    async def execute(self, code, inputs=None, scope=None, print_callback=None):
        lock = asyncio.Event()
        
        inputs = inputs or {}
        if scope:
            inputs.update(self.scopes.get(scope, {}))
        async def st():
            lock.set()
        
        async def pcb(*args):
            self.log.append(args)
            if print_callback:
                await print_callback(*args)

        job = Job(code, inputs, pcb, st, asyncio.get_event_loop())
        self.executor.enqueue(job)
        await lock.wait()
        t, new_vars = job.returns
        if t == 'return':
            if scope:
                if not scope in self.scopes:
                    self.scopes[scope] = {}
                self.scopes[scope].update(new_vars)
            return new_vars
        else:
            raise new_vars

    def stop(self):
        self.executor.stop_lock.set()
        self.interrupt()
        self.lock.set()

    def interrupt(self):
        self.executor.queue.clear()
        if self.executor.call_lock.isSet():
            signal.raise_signal(signal.SIGINT)

    def __repr__(self):
        return f'Instance<{self.name}>'

class Job:
    def __init__(self, code, inputs, print_callback, cb, loop):
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
    
    def enqueue(self, job):
        self.queue.append(job)
        self.call_lock.set()

    async def _execute(self, code, inputs, print_callback, loop):
        prefix = 'async def _wrapper(_new ):\n'
        content = ('\n'+code).replace('\n', '\n ')
        suffix = "\n for _var in dir():\n  if _var[0] != '_':\n   _new[_var] = eval(_var)"
        
        exec(prefix+content+suffix, inputs)
        new_vars = {}
        if print_callback:
            stderr = StdOutCapture(print_callback, loop, True)
            with redirect_stderr(stderr):
                stdout = StdOutCapture(print_callback, loop)
                with redirect_stdout(stdout):
                    await inputs['_wrapper'](new_vars)
        else:
            await inputs['_wrapper'](new_vars)
        return new_vars

    async def run(self):
        while not self.stop_lock.is_set():
            while not self.queue:
                if self.stop_lock.is_set(): return
                self.call_lock = threading.Event()
                self.call_lock.wait(5)
            
            job = self.queue.popleft()
            try:
                r = await self._execute(job.code, job.inputs, job.print_callback, job.loop)
                t = 'return'
            except (KeyboardInterrupt, Exception) as e:
                r = e
                t = 'raise'
            self.future = None
            job.returns = t, r
            asyncio.run_coroutine_threadsafe(job.cb(), job.loop)


def decode_args():
    kwargs = {}
    for env, val in os.environ.items():
        if env.startswith('TELEKINESIS_'):
            key = env[len('TELEKINESIS_'):].lower()
            kwargs[key] = val
    args_order = ('url', 'instance_name', 'private_key_str', 'key_password', 'key_filename', 'route_str')
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
            if i == len(argv):
                kwargs[key] = True
            continue
        if in_kws:
            raise SyntaxError(f'Error parsing arguments: {sys.argv}')
        kwargs[args_order[i]] = arg
    
    return kwargs 


async def start_instance(executor, url, instance_name, private_key_str=None, key_password=None, key_filename=None, route_str=None, **_):
    private_key = None
    if private_key_str:
        private_key = tk.cryptography.PrivateKey.from_private_serial(json.loads(private_key_str).encode(), key_password)
        if key_filename:
            private_key.save_key_file(key_filename, key_password)
    elif key_filename:
        private_key = tk.cryptography.PrivateKey(key_filename, key_password)

    lock = asyncio.Event()
    instance = Instance(instance_name, executor, lock)

    if route_str:
        entrypoint = await tk.Entrypoint(url, private_key)
        route = tk.Route(**json.loads(os.environ['TELEKINESIS_ROUTE']))
        await tk.Telekinesis(route, entrypoint._session)(instance_name, instance)
    else:
        await tk.authenticate(url, private_key).set(instance_name, instance)
    await lock.wait()

def run_in_new_event_loop(future):
    l = asyncio.new_event_loop()
    asyncio.set_event_loop(l)
    
    l.run_until_complete(future)

executor = Executor()

threading.Thread(target=run_in_new_event_loop, args=[start_instance(executor, **decode_args())]).start()

asyncio.run(executor.run())