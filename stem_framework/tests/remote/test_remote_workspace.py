from unittest import TestCase

from stem.remote.remote_workspace import RemoteTask, RemoteWorkspace
from stem.remote.unit import start_unit_in_subprocess

from tests.example_workspace import IntWorkspace

import logging
# logging.root.setLevel(logging.INFO)

HOST = "localhost"
PORT = 9910

class TestRemoteTask(TestCase):
    def setUp(self) -> None:
        self.process, self.server = start_unit_in_subprocess(IntWorkspace, HOST, PORT)

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.process.join()

    def test_of_transform(self):
        remote_task = RemoteTask('data_scale', HOST, PORT)
        res = remote_task.transform({})
        self.assertEqual(res, 10)

class TestRemoteWorkspace(TestCase):
    def setUp(self) -> None:
        self.process, self.server = start_unit_in_subprocess(IntWorkspace, HOST, PORT)
        self.root_remote_workspace = RemoteWorkspace(HOST, PORT)

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.process.join()

    def testTasks(self):
        tasks = self.root_remote_workspace.tasks
        self.assertTrue('int_range_from_func' in tasks)

    def testWorkspaces(self):
        subworkspace = list(self.root_remote_workspace.workspaces)[0]
        self.assertEqual(subworkspace.name, 'SubWorkspace')

        subtasks = subworkspace.tasks
        logging.info(subtasks.keys())
        self.assertTrue('SubWorkspace.int_reduce' in subtasks)