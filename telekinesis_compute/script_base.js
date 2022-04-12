const tk = require('telekinesis-js');
const vm = require('vm');
const esprima = require('esprima')
const { exec } = require('child_process');

class ConsoleCapture {
  constructor(callback) {
    this.callback = callback
  }
  async log() {
    await this.callback(arguments).catch(() => null);
  }
  async error() {
    await this.callback(arguments).catch(() => null);
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
    this._serviceRunner = undefined;
  }
  async execute(code, inputs, outputs, scope, print_callback, inject_context) {
    const randStr = () => '_'+(Math.random() + 1).toString(36).substring(2);
    const syntaxTree = esprima.parse(code).body;
    const mappings = Array.from(syntaxTree.reduce((p, x) => {
        if (x.type === 'ClassDeclaration') {
          p.add(x.id.name);
        } else if (x.type == 'VariableDeclaration') {
          x.declarations.forEach(y => {
            y.id.type === 'Identifier'? p.add(y.id.name) :
              y.id.type === 'ArrayPattern'? y.id.elements.forEach(z => p.add(z.name)) : null
          })
        }
        return p;
      }, new Set()
    )).reduce((p, v) => {p[randStr()] = v; return p}, {});
    const suffix = Object.entries(mappings).map(x => x.join(' = ')).join('\n') + '\n});'

    inputs = inputs || {};
    inputs.require = require;
    inputs.console = new ConsoleCapture(async (...args) => {
      this.log.push(args);
      if (print_callback) {
        await print_callback(...args);
      }
    })
    if (scope && this.scopes[scope]) {
      inputs = {...this.scopes[scope], ...inputs}
    }
    if (inject_context) {
      inputs._tkcContext = contextFactory(this._stopCallback, this._serviceRunner)
    }
    let context = vm.createContext(inputs);
    const content = '(async () => {\n' +code+'\n'+suffix;
    await vm.runInContext(content, context)();
    let out = Object.entries(context).filter(([k, _]) => !['require', 'console'].includes(k))
      .map(([k, v]) => mappings.hasOwnProperty(k) ? [mappings[k], v]: [k, v])
      .reduce((p, v) => {p[v[0]] = v[1]; return p}, {});
    if (scope) {
      this.scopes[scope] = {...this.scopes[scope], ...out};
    }

    if (outputs) {
      if (!(outputs instanceof Array)) {
        return out[outputs];
      } 
      return Object.entries(out).reduce((p, [k, v]) => {
        if (outputs.includes(k)) {p[k] = v;} 
        return p;
      }, {});
    }
    if (scope === undefined) {
      return Object.entries(out).reduce((p, [k, v]) => {
        if (k[0] != '_') {p[k] = v;}
        return p;
      }, {});
    }
  }
  async stop() {
    if (this._stopCallback) {
      await this._stopCallback().catch(() => null);
    }
    this._resolve()
  }
  _updateCallbacks(keepAliveCallback, serviceRunner) {
    this._keepAliveCallback = keepAliveCallback;
    this._serviceRunner = serviceRunner;

    return this;
  }
  _keepAlive(metadata) {
    if (this._keepAliveCallback && metadata?.caller.session[0] != this._keepAliveCallback._target.session[0]) {
      this._keepAliveCallback().then(() => null)
    }
  }
}

const contextFactory = (stopper, runner) => {
  const context = async (...args) => await runner(...args);
  context.stop = async () => await stopper();
  context.execCommand = (cmd) => new Promise((r, re) => {
    exec(cmd, (err, stdout, stderr) => err ? re(stderr || "") : r(stdout || ""));
  });
  return context;
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
        (keepAliveCb, serviceRunner) => pod._updateCallbacks(keepAliveCb, serviceRunner), pod
      );
    });
    entrypoint._session.messageListener = md => pod._keepAlive(md);
  } else {
    tk.authenticate(kwargs.url, privateKey).data.put(pod, kwargs.pod_name).then(() => console.error('>> Pod is running'));
  }
});
main(decodeArgs()).then(() => {throw new Error('Exiting');})
