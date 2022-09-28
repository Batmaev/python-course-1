from functools import reduce
from unittest import TestCase

from stem.task import Task, MapTask, FilterTask, ReduceTask
from tests.example_task import IntRange, int_range, int_scale, data_scale


class TaskTest(TestCase):

    def test_datatask(self):
        task = IntRange()
        self.assertEqual(task.name, "int_range")
        for i, r in zip(range(0, 10, 1), task.data({})):
            self.assertEqual(i, r)

    def test_data_decorator(self):
        self.assertEqual(int_range.__name__, "int_range")
        self.assertEqual(int_range.name, "int_range")
        self.assertTrue(isinstance(int_range, Task))
        for i, r in zip(range(0, 10, 1), int_range.data({})):
            self.assertEqual(i, r)

    def test_task_decorator(self):
        dependencies = set(int_scale.dependencies)
        except_dependencies = {"int_range", "data_scale"}
        self.assertSetEqual(dependencies, except_dependencies)
        for i, r in zip(range(0, 100, 10),
                        int_scale.transform({}, int_range=int_range.data({}), data_scale=data_scale.data({}))):
            self.assertEqual(i, r)

    def test_map_task(self):
        task = MapTask(lambda x: x * 10, int_range)
        self.assertEqual(task.name, "map_int_range")
        for i, r in zip(range(0, 100, 10), task.transform({}, int_range=int_range.data({}))):
            self.assertEqual(i, r)

    def test_filter_task(self):
        task = FilterTask(lambda x: x % 2 == 0, int_range)
        self.assertEqual(task.name, "filter_int_range")
        for i, r in zip(range(0, 10, 2), task.transform({}, int_range=int_range.data({}))):
            self.assertEqual(i, r)

    def test_reduce_task(self):
        task = ReduceTask(lambda acc, x: acc + x, int_range)
        self.assertEqual(task.name, "reduce_int_range")
        self.assertEqual(reduce(lambda acc, x: acc + x, range(0, 10, 1)),
                         task.transform({}, int_range=int_range.data({})))


