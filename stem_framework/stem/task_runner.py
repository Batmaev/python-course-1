import os
from typing import Generic, TypeVar
from abc import ABC, abstractmethod

from .meta import Meta
from .task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 5)


class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)
