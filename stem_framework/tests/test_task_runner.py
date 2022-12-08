from unittest import TestCase

from stem.task import FunctionDataTask, data, task
from stem.task_master import TaskMaster
from stem.task_runner import SimpleRunner, TaskRunner, ThreadingRunner, AsyncRunner, ProcessingRunner
from stem.workspace import Workspace

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
        runner = ThreadingRunner(5)
        self._run(runner)

    def test_async(self):
        runner = AsyncRunner()
        self._run(runner)


    def test_process(self):
        # Поскольку multiprocessing использует pickle,
        # а pickle не может работать с декораторами, лямбда-функциями, замыканиями и тд
        # будем тестировать этот раннер на специальном безопасном воркспейсе

        runner = ProcessingRunner()
        master = TaskMaster(runner)
        result = master.execute({}, int_scale_task)
        print(result.status)

        for i, r in zip(range(0, 100, 10), result.data):
            self.assertEqual(i, r)



# Picklable Workspace

def int_scale_func(meta, int_range, data_scale):
    return [data_scale * x for x in int_range]

int_scale_task = task(int_scale_func)


def int_range_func(meta):
    return list(range(10))

int_range = FunctionDataTask('int_range', int_range_func)


def data_scale_func(meta):
    return 10

data_scale = FunctionDataTask('data_scale', data_scale_func)