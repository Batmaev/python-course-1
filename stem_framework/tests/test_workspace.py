from unittest import TestCase

from stem.workspace import Workspace, IWorkspace, ProxyTask, LocalWorkspace
from tests.example_task import int_range
from tests.example_workspace import IntWorkspace, SubWorkspace, SubSubWorkspace


class WorkspaceTest(TestCase):

    def setUp(self) -> None:
        self.workspace = IntWorkspace

    def test_instance(self):
        self.assertIs(self.workspace, IntWorkspace())

    def test_name(self):
        self.assertEqual("IntWorkspace", IntWorkspace.name)
        self.assertEqual("IntWorkspace", self.workspace.name)

    def test_subclass(self):
        self.assertFalse(isinstance(IntWorkspace, type))
        self.assertTrue(isinstance(IntWorkspace, IWorkspace))
        self.assertTrue(issubclass(Workspace, type))
        self.assertTrue(issubclass(Workspace, IWorkspace))

    def test_task(self):
        tasks = [
            "int_range_from_class", "int_range_from_func",
            "data_scale", "int_scale", "int_range_as_method"
        ]
        for task in tasks:
            with self.subTest(task):
                self.assertTrue(task in IntWorkspace.tasks)
                self.assertTrue(IntWorkspace.has_task(task))

        isinstance(IntWorkspace.int_range_from_class, ProxyTask)
        isinstance(IntWorkspace.int_range_from_func, ProxyTask)

    def test_workspace(self):
        self.assertIn(SubSubWorkspace, SubWorkspace.workspaces)
        self.assertIn(SubWorkspace, IntWorkspace.workspaces)

    def test_subtask(self):
        sub_tasks = ["int_reduce", "sub_sub_int_range"]
        for task in sub_tasks:
            with self.subTest(task):
                self.assertTrue(IntWorkspace.has_task(task))

    def test_default_workspace(self):

        workspace = Workspace.find_default_workspace(int_range)
        self.assertTrue(isinstance(workspace, LocalWorkspace))
        self.assertEqual("example_task", workspace.name)
        self.assertIn("int_scale", workspace.tasks)
        self.assertIn("data_scale", workspace.tasks)

        workspace = Workspace.find_default_workspace(IntWorkspace.int_range_from_class)
        self.assertIs(workspace, self.workspace)

    def test_structure(self):
        ref = {'name': 'IntWorkspace', 'tasks': ['int_range_from_class', 'int_range_from_func',
                                                 'int_range_as_method', 'data_scale', 'int_scale'],
               'workspaces': [{'name': 'SubWorkspace', 'tasks': ['int_reduce'],
                               'workspaces': [{'name': 'SubSubWorkspace',
                                               'tasks': ['sub_sub_int_range'],
                                               'workspaces': []}]}]}
        self.assertDictEqual(ref, IntWorkspace.structure())
