const tk = require('telekinesis-js');
const vm = require('vm');

const main = () => new Promise(resolve => {
    console.log('starting')

    async function executor(code, namespace, injectContext=False) {
        namespace = namespace || {};
        if (injectContext) {
            namespace['_context'] = {'stop': resolve}
        }
        let context = vm.createContext(namespace)
        content = '(async () => {\n' +code+"\n})();";
        vm.runInContext(content, context)
        return context;
    }

    let entrypoint = await new tk.Entrypoint(process.env.URL, JSON.parse(process.env.PRIVATEKEY.replace('\\', '\n')));
    let route = tk.Route.fromObject(JSON.parse(process.env.ROUTE))

    await new tk.Telekinesis(route, entrypoint._session)({executor: executor, stop: resolve})

    console.log('running');
});

main().then(console.log);