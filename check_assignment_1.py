"""
Auto check of assignment 1
"""
import os
import pathlib
from importlib.machinery import SourceFileLoader
from subprocess import Popen, PIPE

def main():
    root = pathlib.Path(".")

    stem_framework = root / "stem_framework"
    temperature_monitor = root / "temperature_monitor"
    assert stem_framework.exists()
    assert temperature_monitor.exists()

    for path in [stem_framework, temperature_monitor]:
        assert (path / "setup.py").exists()
        assert (path / "LICENSE").exists()
        assert (path / "stem").exists()
        assert (path / "stem" / "__init__.py").exists()

    stem = stem_framework / "stem"
    
    for name in ["core.py", "meta.py", "task.py", "workspace.py"]:
        loader = SourceFileLoader(name[:-3], str(stem/name))
        module = loader.load_module()
        print("\n", module.__doc__, "\n")
    
    os.chdir(stem_framework)

    commands = [
        ["pip", "install", "-e", "."],
        ["python", "setup.py", "build_sphinx"]
    ]
    for command in commands:
        with Popen(command, stdout=PIPE, stderr=PIPE) as proc:
            outs, errs = proc.communicate()
            print(errs)



if __name__ == '__main__':
    main()