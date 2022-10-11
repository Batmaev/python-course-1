import argparse
import cmd

from .workspace import IWorkspace


def print_structure(workspace: IWorkspace, args: argparse.Namespace):
    def pretty(d, indent=0):
        for key, value in d.items():
            print('\t' * indent + str(key))
            if isinstance(value, dict):
                pretty(value, indent + 1)
            else:
                print('\t' * (indent + 1) + str(value))

    pretty(workspace.structure())


def run_task(workspace: IWorkspace, args: argparse.Namespace):
    pass  # TODO()


def create_parser() -> argparse.ArgumentParser:
    pass  # TODO()


def stem_cli_main():
    pass  # TODO()
