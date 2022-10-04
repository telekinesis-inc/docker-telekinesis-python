const tk = require('telekinesis-js');
const vm = require('vm');
const { exec } = require('child_process');

class ConsoleCapture {
  constructor(callback) {
    this.callback = callback
  }
  async log() {
    await this.callback(...arguments).catch(() => null);
  }
  async error() {
    await this.callback(...arguments).catch(() => null);
  }
}

class Pod {
  constructor(name, resolve) {
    this.name = name;
    this.calls = [];
    this._resolve = resolve;
    this._stopCallback = undefined;
    this._keepAliveCallback = undefined
  }
  async execute(code, inputs, consoleLogCallback, secret=false) {
    const timestamp = Date.now();
    const callData = {
      code: secret ? '<HIDDEN>' : code, inputs: secret ? '<HIDDEN>' : inputs,
      status: 'RUNNING', log: {}
    };
    this.calls.push([timestamp, callData])
    const suffix = '\n});'

    inputs = {...inputs} || {};
    inputs.require = require;
    inputs.console = new ConsoleCapture(async (...args) => {
      callData.log[Date.now()] = Object.values(args);
      if (consoleLogCallback) {
        await consoleLogCallback(...args);
      }
    })
    let context = vm.createContext(inputs);
    const content = '(async () => {\n' +code+'\n'+suffix;
    try {
      callData.output = await vm.runInContext(content, context)();
      callData.status = 'SUCCEEDED';
      return callData.output
    } catch(e) {
      callData.output = e;
      callData.status = 'ERROR';
      throw e;
    }
  }
  async stop() {
    if (this._stopCallback) {
      await this._stopCallback().catch(() => null);
    }
    this._resolve()
  }
  _updateCallbacks(keepAliveCallback, serviceRunner, name) {
    this._keepAliveCallback = keepAliveCallback;
    this._serviceRunner = serviceRunner;
    if (name) {
      this.name = name;
    }

    return this;
  }
  toString() {
    return 'Pod('+this.name+')'
  }
  _keepAlive(metadata) {
    if (this._keepAliveCallback && metadata?.caller.session[0] != this._keepAliveCallback._target.session[0]) {
      this._keepAliveCallback().then(() => null)
    }
  }
}


function decodeArgs() {
  let kwargs = {}
  for (let [env, val] of Object.entries(process.env)) {
    if (env.slice(0, 'TELEKINESIS_'.length) === 'TELEKINESIS_') {
      let key = env.slice('TELEKINESIS_'.length).toLowerCase();
      kwargs[key] = val;
    }
  }
  const argsOrder = ['url', 'pod_name', 'private_key_str', 'key_password', 'key_filename', 'route_str'];
  const argv = process.argv.slice(2);
  let key = null;
  let inKws = false
  for (let i in argv) {
    let arg = argv[i];
    if (key) {
      if (arg.slice(0,2) === '--') {
        kwargs[key] = true;
      } else {
        kwargs[key] = arg;
        key = null;
        continue;
      }
    }
    if (arg.slice(0,2) === '--') {
      inKws = true;
      key = arg.slice(2);
      if (i === argv.length-1) {
        kwargs[key] = true;
      }
      continue;
    }
    if (inKws) {
      throw 'Error parsing arguments' + process.argv;
    }
    kwargs[argsOrder[i]] = arg;
  }
  return kwargs 
}

const main = (kwargs) => new Promise(resolve => {
  if (!('url' in kwargs)) {throw 'Missing url argument'}
  if (!('pod_name' in kwargs)) {throw 'Missing pod_name argument'}

  let pod = new Pod(kwargs.pod_name, resolve);
  let privateKey = 'private_key_str' in kwargs? JSON.parse(kwargs.private_key_str) : undefined;

  if (kwargs.route_str) {
    let entrypoint = new tk.Entrypoint(kwargs.url, privateKey);
    let route = tk.Route.fromObject(JSON.parse(kwargs.route_str));
    // entrypoint.then(async () => await new tk.Telekinesis(route, entrypoint._session)(console.error || 0, pod));
    entrypoint.then(async () => {
      pod._stopCallback = await new tk.Telekinesis(route, entrypoint._session)(
        (keepAliveCb, serviceRunner, name) => pod._updateCallbacks(keepAliveCb, serviceRunner, name), pod
      );
    });
    entrypoint._session.messageListener = md => pod._keepAlive(md);
  } else {
    tk.authenticate(kwargs.url, privateKey).data.put(pod, kwargs.pod_name).then(() => console.error('>> Pod is running'));
  }
});
main(decodeArgs()).then(() => {throw new Error('Exiting');})
