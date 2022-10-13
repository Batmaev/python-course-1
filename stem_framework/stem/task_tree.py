from typing import TypeVar, Optional, Generic

from .task import Task
from .workspace import IWorkspace

T = TypeVar("T")


class TaskNode(Generic[T]):

    @property
    def dependencies(self) -> list["TaskNode"]:
        return self._dependencies

    @property
    def is_leaf(self) -> bool:
        return self.dependencies == []

    @property
    def unresolved_dependencies(self) -> list["str"]:
        return self._unresolved_dependencies

    @property
    def has_dependence_errors(self) -> bool:
        return self._has_dependence_errors

    def __init__(self, task: Task, workspace: IWorkspace | None = None):
        if workspace is not None:
            workspace_ = workspace
        else:
            workspace_ = IWorkspace.find_default_workspace(task)


        self.task = task
        self._dependencies = []
        self._unresolved_dependencies = []
        self.workspace = workspace

        for d in task.dependencies: # d: Task | str
            if isinstance(d, Task):
                self._dependencies.append(TaskNode(d, workspace))
            elif (t := workspace_.find_task(d)) is not None:
                self._dependencies.append(TaskNode(t, workspace))
            else:
                self._unresolved_dependencies.append(d)

        self._has_dependence_errors = self._unresolved_dependencies != [] or any(d._has_dependence_errors for d in self.dependencies)

    def resolve_node(self, task: Task[T], workspace: IWorkspace | None = None) -> Optional["TaskNode[T]"]:
        """Ищет task в дереве/корневом узле self. 

        Если задан аругмент workspace и он не тот, который использовался при создании дерева,
        создаётся новое одноразовое дерево и поиск осуществляется в нём.

        Если узла не обнаружено, то возвращается None."""

        if workspace is not None and workspace != self.workspace:
            self = TaskNode(task, workspace)
        if self.task == task:
            return self
        for d in self.dependencies:
            if (node := d.resolve_node(task)) is not None:
                return node


class TaskTree(TaskNode): # code reuse

    @staticmethod
    def build_node(task: Task):
        return TaskNode(task)
