from unittest import TestCase

from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner
from tests.example_task import int_scale


class SimpleRunnerTest(TestCase):

    def setUp(self) -> None:
        self.runner = SimpleRunner()

    def test_execute(self):
        task_master = TaskMaster(self.runner)
        result = task_master.execute({}, int_scale)
        for i, r in zip(range(0, 100, 10), result.lazy_data()):
            self.assertEqual(i, r)