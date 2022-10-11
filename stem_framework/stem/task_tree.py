from typing import TypeVar, Optional, Generic

from .task import Task
from .workspace import IWorkspace

T = TypeVar("T")


class TaskNode(Generic[T]):
    task: Task[T]

    @property
    def dependencies(self) -> list["TaskNode"]:
        pass  # TODO()

    @property
    def is_leaf(self) -> bool:
        pass  # TODO()

    @property
    def unresolved_dependencies(self) -> list["str"]:
        pass # TODO()

    @property
    def has_dependence_errors(self) -> bool:
        pass  # TODO()


class TaskTree:

    def resolve_node(self, task: Task[T], workspace: Optional[IWorkspace] = None) -> TaskNode[T]:
        pass  # TODO()
