const tk = require('telekinesis-js');
const vm = require('vm');

const main = () => new Promise(resolve => {
    console.log('starting')

    async function executor(code, namespace, injectContext=false) {
        namespace = namespace || {};
        if (injectContext) {
            namespace['_context'] = {stop: resolve}
        }
        namespace.require = require;
        let context = vm.createContext(namespace)
        content = '(async () => {\n' +code+"\n})";
        await vm.runInContext(content, context)();
        return context;
    }
    let route = tk.Route.fromObject(JSON.parse(process.env.ROUTE))

    let entrypoint = new tk.Entrypoint(process.env.URL, process.env.PRIVATEKEY.replaceAll('\\', '\n'));

    entrypoint.then(async () => await new tk.Telekinesis(route, entrypoint._session)({execute: executor, stop: resolve}))

    console.log('running');
});
main().then(console.log)