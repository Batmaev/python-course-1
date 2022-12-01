import logging
import os
import socketserver
from socketserver import StreamRequestHandler
from typing import Optional

from stem.envelope import Envelope
from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner
from stem.task_tree import TaskTree
from stem.workspace import IWorkspace
from multiprocessing import Process


class UnitHandler(StreamRequestHandler):
    workspace: IWorkspace
    task_tree: TaskTree = TaskTree()
    task_master = TaskMaster(SimpleRunner(), task_tree)
    powerfullity: int

    def handle(self) -> None:
        pass  # TODO(Assigment 10)


def start_unit(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None):
    pass  # TODO(Assigment 10)


def start_unit_in_subprocess(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None) -> Process:
    pass  # TODO(Assigment 10)
