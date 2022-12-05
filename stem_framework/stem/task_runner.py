import os
import asyncio
from concurrent import futures
from typing import Generic, TypeVar
from abc import ABC, abstractmethod

from .meta import Meta, get_meta_attr
from .task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        assert not task_node.has_dependence_errors
        kwargs = {
            t.task.name: self.run(
                get_meta_attr(meta, t.task.name, {}),
                t
            )
            for t in task_node.dependencies
        }
        return task_node.task.transform(meta, **kwargs)


class ThreadingRunner(TaskRunner[T]):

    def __init__(self, MAX_WORKERS) -> None:
        self.MAX_WORKERS = MAX_WORKERS

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        with futures.ThreadPoolExecutor(self.MAX_WORKERS) as executor:
            return self._run(meta, task_node, executor).result()

    def _run(self, meta: Meta, task_node: TaskNode[T], executor: futures.Executor):
        kwargs = {
            t.task.name: self._run(
                get_meta_attr(meta, t.task.name, {}),
                t,
                executor
            )
            for t in task_node.dependencies
        }
        futures.wait(kwargs.values())
        kwargs = {k: v.result() for k, v in kwargs.items()}
        return executor.submit(task_node.task.transform, meta, **kwargs)


class ProcessingRunner(ThreadingRunner[T]):

    def __init__(self) -> None:
        self.MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        with futures.ProcessPoolExecutor(self.MAX_WORKERS) as executor:
            return self._run(meta, task_node, executor).result()


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        return asyncio.run(self._run(meta, task_node))

    async def _run(self, meta: Meta, task_node: TaskNode[T]):
        async with asyncio.TaskGroup() as tg:
            kwargs = {
                t.task.name: tg.create_task(
                    self._run(get_meta_attr(meta, t.task.name, {}), t)
                )
                for t in task_node.dependencies
            }
        kwargs = {k: v.result() for k, v in kwargs.items()}
        return task_node.task.transform(meta, **kwargs)