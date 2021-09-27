import os

path = os.path.dirname(os.path.abspath(__file__))

def prepare_files(*dependencies):
    with open(os.path.join(path, 'Dockerfile_base'), 'r') as file_in:
        base = file_in.read()
        out = base.replace('{{DEPENDENCIES}}', '\n'.join('RUN pip install '+ d for d in dependencies))

        with open(os.path.join(path, 'Dockerfile'), 'w') as file_out:
            file_out.write(out)

    with open(os.path.join(path, 'script_base.py'), 'r') as file_in:
        base = file_in.read()
        out = base.replace('{{IMPORTS}}', '\n'.join('import '+ d for d in dependencies))

        with open(os.path.join(path, 'script.py'), 'w') as file_out:
            file_out.write(out)