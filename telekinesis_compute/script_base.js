const tk = require('telekinesis-js');
const vm = require('vm');

class ConsoleCapture {
  constructor(callback) {
    this.callback = callback
  }
  async log() {
    await this.callback(arguments);
  }
  async error() {
    await this.callback(arguments);
  }
}

class Pod {
  constructor(name, resolve) {
    this.name = name;
    this.scopes = {};
    this.log = [];
    this._resolve = resolve;
    this._stopCallback = undefined;
    this._keepAliveCallback = undefined
  }
  async execute(code, inputs, scope, logCallback) {
    inputs = inputs || {};
    inputs.require = require;
    inputs.console = new ConsoleCapture(async (...args) => {
      this.log.push(args);
      if (logCallback) {
        await logCallback(...args);
      }
    })
    if (scope && this.scopes[scope]) {
      inputs = {...this.scopes[scope], ...inputs}
    }
    let context = vm.createContext(inputs);
    const content = '(async () => {\n' +code+"\n})";
    await vm.runInContext(content, context)();
    let out = Object.entries(context).filter(([k, _]) => !['require', 'console'].includes(k)).reduce((p, v) => {p[v[0]] = v[1]; return p}, {}); 
    if (scope) {
      this.scopes[scope] = {...this.scopes[scope], ...out};
    }

    return out;
  }
  async stop() {
    if (this._stopCallback) {
      await this._stopCallback();
    }
    this._resolve()
  }
  _updateCallbacks(stopCallback, keepAliveCallback) {
    this._stopCallback = stopCallback;
    this._keepAliveCallback = keepAliveCallback;
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
    entrypoint.then(async () => await new tk.Telekinesis(route, entrypoint._session)((scb, kacb) => pod._updateCallbacks(scb, kacb), pod));
    entrypoint._session.messageListener = md => pod._keepAlive(md);
  } else {
    tk.authenticate(kwargs.url, privateKey).data.put(pod, kwargs.pod_name).then(() => console.error('>> Pod is running'));
  }
});
main(decodeArgs()).then(() => {throw new Error('Exiting');})
