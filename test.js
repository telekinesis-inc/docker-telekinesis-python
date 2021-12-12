const tk = require('telekinesis-js');
const vm = require('vm');

const main = () => new Promise(resolve => {
    async function executor(code, namespace, injectContext) {
        namespace = namespace || {};
        if (injectContext) {
            namespace['_context'] = {'stop': resolve}
        }
        let context = vm.createContext(namespace)
        content = '(async () => {\n' +code+"\n})();";
        vm.runInContext(content, context)
        return context;
    }
    executor("await y.log(123);", {y: new tk.Telekinesis(console, new tk.Session())});
    resolve(process.env.ZZZ);
});

main().then(console.log)