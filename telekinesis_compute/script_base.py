import sys
import asyncio
import os
import signal
import json
import re
from contextlib import redirect_stdout, redirect_stderr
from collections import deque
import threading

import telekinesis as tk


def _extract_line_blocks(s, _t=None):
    start = False
    if not s:
        if _t is None:
            return [["", False]]
        raise SyntaxError('EOL while scanning string literal')
    if s[0] in ['"', "'"]:
        if _t == s[0]: # close string
            return [_t, True], *_extract_line_blocks(s[1:])
        elif _t is None:
            _t = s[0]
            start = True
    
    [ss, _], *tail = _extract_line_blocks(s[1:], _t)
    out = [s[0]+ss, _t is not None], *tail
    if start:
        return ["", True], *out
    return out


def _preprocess_code(code, mappings):
    blocks = []
    for i, block in enumerate(code.split('"""')):
        if i%2 == 0:
            lines = []
            for line in block.split('\n'):
                line_blocks = _extract_line_blocks(line)
                ending = ''
                for lb in line_blocks:
                    if not lb[1]: # not string literal
                        x, *comment = lb[0].split('#')
                        # print(x, comment)
                        for token, mapping in mappings.items():
                            if not ending and re.match(f'^\s*{token}\s*(?=[^= ])', lb[0]):
                                x = re.sub(f'^\s*{token}\s*(?=[^= ])','await '+mapping[0], x)
                                ending = mapping[1]
                            if not ending and re.match(f'.*{token}\s*(?=[^= ])', x):
                                
                                x = re.sub(f'{token}\s*(?=[^= ])',
                                               'await '+mapping[0], x)
                                ending = mapping[2]
                        if comment:
                            lb[0] = '#'.join([x, ending, *comment])
                            ending = ''
                            break
                        else:
                            lb[0] = x
                    elif ending:
                        lb[0] = '\\' + lb[0][:-1] + '\\' + lb[0][-1]

                lines.append(''.join([*[lb[0] for lb in line_blocks], ending]))
            blocks.append('\n'.join(lines))
        else:
            blocks.append(block)
    return '"""'.join(blocks)


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


class Context:
    def __init__(self, stop):
        self.stop = stop

    async def exec_command(self, cmd, stream_print=False):
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
        
        if stream_print:
            await _stream_subprocess(cmd, print, lambda *x: print(*x, file=sys.stderr))
        else:
            process = await asyncio.create_subprocess_shell(cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            return (await process.stdout.read()).decode().split('\n')


class Pod:
    def __init__(self, name, executor, lock):
        self.name = name
        self.scopes = {}
        self._executor = executor
        self.log = []
        self._lock = lock
        self._stop_callback = None
        self._keep_alive_callback = None

    async def execute(self, code, inputs=None, scope=None, print_callback=None, inject_context=False):
        lock = asyncio.Event()

        inputs = inputs or {}
        if scope:
            inputs.update({k: v for k, v in self.scopes.get(scope, {}).items() if k not in inputs.keys()})
        if inject_context and '_tkc_context' not in inputs:
            inputs['_tkc_context'] = Context(self.stop)
        if inject_context:
            code = _preprocess_code(code, {
                '!': ('_tkc_context.exec_command("', '", stream_print=True)', '")'), 
                '$': ('_tkc_context', '', '')
            })

        async def lock_set():
            lock.set()

        async def print_cb(*args):
            self.log.append(args)
            if print_callback:
                await print_callback(*args)

        job = Job(code, inputs, print_cb, lock_set, asyncio.get_event_loop())
        self._executor.enqueue(job)
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

    async def stop(self):
        self._executor.stop_lock.set()
        self.interrupt()
        if self._stop_callback:
            await self._stop_callback()
        self._lock.set()

    def interrupt(self):
        self._executor.queue.clear()
        if self._executor.call_lock.is_set():
            os.kill(os.getpid(), signal.SIGINT)


    async def _exec_command(self, command, print_callback=None):
        return await self.execute('!'+command, print_callback=print_callback, inject_context=True)

    async def install_package(self, package_name, print_callback=None):
        return await self._exec_command('pip install '+ package_name, print_callback)

    def _update_callbacks(self, stop_callback, keep_alive_callback):
        self._stop_callback = stop_callback
        self._keep_alive_callback = keep_alive_callback

    def _keep_alive(self, metadata):
        if self._keep_alive_callback and metadata.caller.session[0] != self._keep_alive_callback._target.session[0]:
            asyncio.create_task(self._keep_alive_callback()._execute())

    def __repr__(self):
        return f'Pod({self.name})'


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
        prefix = f'async def _tkc_wrapper({", ".join(["_tkc_new_vars", *[k for k in inputs]])}):\n'
        content = ('\n'+code).replace('\n', '\n ')
        suffix = "\n for _var in dir():\n  if _var[0] != '_':\n   _tkc_new_vars[_var] = eval(_var)"

        tmp = {}
        exec(prefix+content+suffix, tmp)
        new_vars = {}
        if print_callback:
            stderr = StdOutCapture(print_callback, loop, True)
            with redirect_stderr(stderr):
                stdout = StdOutCapture(print_callback, loop)
                with redirect_stdout(stdout):
                    await tmp['_tkc_wrapper'](new_vars, **inputs)
        else:
            await tmp['_tkc_wrapper'](new_vars, **inputs)
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
        await tk.Telekinesis(route, entrypoint._session)(pod._update_callbacks, pod)
        entrypoint._session.message_listener = pod._keep_alive
    else:
        await tk.authenticate(url, private_key).data.put(pod, pod_name)
    
    await lock.wait()


def run_in_new_event_loop(future):
    l = asyncio.new_event_loop()
    asyncio.set_event_loop(l)

    l.run_until_complete(future)


executor = Executor()

threading.Thread(target=run_in_new_event_loop, args=[start_pod(executor, **decode_args())]).start()

asyncio.run(executor.run())

