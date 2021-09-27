import asyncio
import telekinesis as tk
import os
{{IMPORTS}}

class Dict(dict):
    def update(*args):
        raise PermissionError
    def pop(*args):
        raise PermissionError
    def popitem(*args):
        raise PermissionError
    def clear(*args):
        raise PermissionError

async def executor(code, namespace={}):
    prefix = 'async def _wrapper(_new):\n'
    content = ('\n'+code).replace('\n', '\n  ')
    suffix = """
    for _var in dir():
        if _var[0] != '_':
            _new[_var] = eval(_var)"""
    exec(prefix+content+suffix, namespace)
    new_vars = {}
    await namespace['_wrapper'](new_vars)
    return Dict(new_vars)

async def main():
    print('starting')
    e = asyncio.Event()

    b = await tk.Broker().serve('0.0.0.0')
    r, t = await tk.create_entrypoint(lambda: {'executor': executor, 'stop': lambda: e.set()}, is_public=False)

    r = await t._delegate(os.environ['PARENT'])
    b.entrypoint = r
    print('running')

    await e.wait()
    print('stopping')

asyncio.run(main())