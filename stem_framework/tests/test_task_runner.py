from unittest import TestCase

from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner, TaskRunner, ThreadingRunner, AsyncRunner, ProcessingRunner
from tests.example_task import int_scale


class RunnerTest(TestCase):

    def _run(self, runner: TaskRunner):
        task_master = TaskMaster(runner)
        result = task_master.execute({}, int_scale)
        for i, r in zip(range(0, 100, 10), result.data):
            self.assertEqual(i, r)

    def test_simple(self):
        runner = SimpleRunner()
        self._run(runner)

    def test_threading(self):
        runner = ThreadingRunner()
        self._run(runner)

    def test_async(self):
        runner = AsyncRunner()
        self._run(runner)

    def test_process(self):
        runner = ProcessingRunner()
        self._run(runner)