from enum import Enum, auto
from typing import Optional, Callable, TypeVar, Generic
from functools import cached_property
from dataclasses import dataclass, field

from .meta import Meta, MetaVerification, Specification
from .task import Task
from .workspace import Workspace
from .task_runner import TaskRunner, SimpleRunner
from .task_tree import TaskNode, TaskTree

T = TypeVar("T")


@dataclass
class TaskMetaError(Generic[T]):
    task_node: TaskNode[T]
    meta_error: Optional[MetaVerification] = None
    user_handler_error: Optional[Exception] = None
    dependencies_error: list["TaskMetaError"] = field(default_factory=list)

    @property
    def task(self) -> Task[T]:
        return self.task_node.task

    @property
    def specification(self) -> Specification:
        return self.task.specification

    @property
    def has_error(self) -> bool:
        return self.meta_error is not None or \
               any(map(lambda x: x.has_error, self.dependencies_error))


class TaskStatus(Enum):
    DEPENDENCIES_ERROR = auto()
    META_ERROR = auto()
    INVOCATION_ERROR = auto()
    CONTAINS_DATA = auto()


@dataclass
class TaskResult(Generic[T]):
    status: TaskStatus
    task_node: TaskNode[T]
    meta_errors: Optional[TaskMetaError] = None
    lazy_data: Callable[[], T] = lambda: None

    @cached_property
    def data(self) -> Optional[T]:
        try:
            return self.lazy_data()
        except Exception as e:
            self.status = TaskStatus.INVOCATION_ERROR
            raise e


class TaskMaster:

    def __init__(self, task_runner: TaskRunner[T] = SimpleRunner(), task_tree: Optional[TaskTree] = None):
        self.task_runner = task_runner
        self.task_tree = task_tree

    def execute(self, meta: Meta, task: Task[T], workspace: Optional[Workspace] = None) -> TaskResult[T]:
        pass  # TODO()
