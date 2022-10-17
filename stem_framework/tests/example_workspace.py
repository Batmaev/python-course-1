from functools import reduce
from typing import Iterator

from stem.meta import Meta, get_meta_attr
from stem.task import data, task
from stem.workspace import Workspace
from tests.example_task import IntRange, int_range


class SubSubWorkspace(metaclass=Workspace):
    sub_sub_int_range = IntRange()


class SubWorkspace(metaclass=Workspace):
    workspaces = [SubSubWorkspace]

    @task
    @staticmethod
    def int_reduce(meta: Meta, int_scale: Iterator[int]) -> int:
        return reduce(lambda x, y: x + y, int_scale)


class IntWorkspace(metaclass=Workspace):

    workspaces = [SubWorkspace]

    int_range_from_class = IntRange()

    int_range_from_func = int_range

    # Classes with metaclass == Workspace
    # shouldn't have methods which accept self,
    # because these classes cannot be instantiated
    # as the assignment requires.
    #
    # Such classes can only have @staticmethods and @classmethods

    @data
    @staticmethod
    def int_range_as_method(meta: Meta) -> Iterator[int]:
        """Source of integer number"""
        opts = get_meta_attr(meta, "start", 0), get_meta_attr(meta, "stop", 10), get_meta_attr(meta, "step", 1)
        for i in range(*opts):
            yield i

    @data
    @staticmethod
    def data_scale(meta: Meta) -> int:
        return 10

    @task
    @staticmethod
    def int_scale(meta: Meta, int_range: Iterator[int], data_scale: int) -> Iterator[int]:
        return map(lambda x: data_scale*x, int_range)