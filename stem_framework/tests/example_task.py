from functools import reduce
from typing import Iterator

import numpy as np

from stem.meta import Meta, get_meta_attr
from stem.task import DataTask, data, task


class IntRange(DataTask):
    def data(self, meta: Meta) -> Iterator[int]:
        opts = get_meta_attr(meta, "start", 0), get_meta_attr(meta,"stop", 10), get_meta_attr(meta, "step", 1)
        for i in range(*opts):
            yield i


@data
def int_range(meta: Meta)-> Iterator[int]:
    """Source of ineteger number"""
    opts = get_meta_attr(meta, "start", 0), get_meta_attr(meta, "stop", 10), get_meta_attr(meta, "step", 1)
    for i in range(*opts):
        yield i


@data
def float_range(meta: Meta) -> Iterator[float]:
    """Source of double number"""
    opts = meta.get("start", 0), meta.get("stop", 1), meta.get("step", 0.1)
    for i in np.arange(*opts, dtype="f"):
        yield i


@data
def data_scale(meta: Meta) -> int:
    return 10


@task
def int_scale(meta: Meta, int_range: Iterator[int], data_scale: int) -> Iterator[int]:
    return map(lambda x: data_scale*x, int_range)


@task
def int_reduce(meta: Meta, int_scale: Iterator[int]) -> int:
    return reduce(lambda x,y: x + y, int_scale)


@task
def float_scale(meta: Meta, int_reduce: int, float_range: Iterator[float]) -> Iterator[float]:
    return map(lambda x: int_reduce*x, float_range)


@task
def float_reduce(meta: Meta, float_scale: Iterator[float]) -> float:
    return sum(float_scale)
