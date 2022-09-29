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
    def int_reduce(self, meta: Meta, int_scale: Iterator[int]) -> int:
        return reduce(lambda x, y: x + y, int_scale)


class IntWorkspace(metaclass=Workspace):

    workspaces = [SubWorkspace]

    int_range_from_class = IntRange()

    int_range_from_func = int_range

    @data
    def int_range_as_method(self, meta: Meta) -> Iterator[int]:
        """Source of integer number"""
        opts = get_meta_attr(meta, "start", 0), get_meta_attr(meta, "stop", 10), get_meta_attr(meta, "step", 1)
        for i in range(*opts):
            yield i

    @data
    def data_scale(self, meta: Meta) -> int:
        return 10

    @task
    def int_scale(self, meta: Meta, int_range: Iterator[int], data_scale: int) -> Iterator[int]:
        return map(lambda x: data_scale*x, int_range)
