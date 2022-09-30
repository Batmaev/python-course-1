from abc import abstractmethod, ABC, ABCMeta
from types import ModuleType
from typing import Optional, Any, TypeVar, Union
from typing_extensions import Self
from importlib import import_module

from .core import Named
from .meta import Meta
from .task import Task

T = TypeVar("T")


class TaskPath:
    def __init__(self, path: Union[str, list[str]]):
        if isinstance(path, str):
            self._path = path.split(".")
        else:
            self._path = path

    @property
    def is_leaf(self):
        return len(self._path) == 1

    @property
    def sub_path(self):
        return TaskPath(self._path[1:])

    @property
    def head(self):
        return self._path[0]

    @property
    def name(self):
        return self._path[-1]

    def __str__(self):
        return ".".join(self._path)


class ProxyTask(Task[T]):

    def __init__(self, proxy_name, task: Task):
        self._name = proxy_name
        self._task = task

    @property
    def dependencies(self):
        return self._task.dependencies

    @property
    def specification(self):
        return self._task.specification

    def check_by_meta(self, meta: Meta):
        self._task.check_by_meta(meta)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._task.transform(meta, **kwargs)


class IWorkspace(ABC, Named):

    @property
    @abstractmethod
    def tasks(self) -> dict[str, Task]:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> set["IWorkspace"]:
        pass

    @classmethod
    def find_task(cls, task_path: Union[str, TaskPath]) -> Optional[Task]:
        if not isinstance(task_path, TaskPath):
            task_path = TaskPath(task_path)
        if not task_path.is_leaf:
            for w in cls.workspaces:
                if w.name == task_path.head:
                    return w.find_task(task_path.sub_path)
            return None
        else:
            for task_name in cls.tasks:
                if task_name == task_path.name:
                    return cls.tasks[task_name]
            for w in cls.workspaces:
                if t := w.find_task(task_path) is not None:
                    return t
            return None

    @classmethod
    def has_task(cls, task_path: Union[str, TaskPath]) -> bool:
        return cls.find_task(task_path) is not None

    def get_workspace(self, name) -> Optional["IWorkspace"]:
        for workspace in self.workspaces:
            if workspace.name == name:
                return workspace
        return None

    @classmethod
    def structure(cls) -> dict:
        return {
            "name": cls.name,
            "tasks": list(cls.tasks.keys()),
            "workspaces": [w.structure() for w in cls.workspaces]
        }

    @staticmethod
    def find_default_workspace(task: Task) -> "IWorkspace":
        try:
            return task._stem_workspace
        except AttributeError:
            module = import_module(task.__module__)
            return IWorkspace.module_workspace(module)

    @staticmethod
    def module_workspace(module: ModuleType) -> "IWorkspace":
        try:
            return module.__stem_workspace

        except AttributeError:

            tasks = {}
            workspaces = set()

            for s in dir(module):
                t = getattr(module, s)
                if isinstance(t, Task):
                    tasks[s] = t
                if isinstance(t, IWorkspace):
                    workspaces.add(t)

            module.__stem_workspace = LocalWorkspace(
                module.__name__, tasks, workspaces
            )

            return module.__stem_workspace




class ILocalWorkspace(IWorkspace):

    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):

    def __init__(self, name,  tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta, ILocalWorkspace):
    def __new__(mcls: type[Self], name: str, bases: tuple[type, ...], namespace: dict[str, Any], **kwargs: Any) -> Self:

        # Method Resolution Order:
        #
        # Workspace             # calls super().__new__
        #   ABCMeta             # calls super().__new__
        #       type            # __new__ is executed and super()-sequence stops
        #   ILocalWorkspace
        #       IWorkspace
        #           ABC
        #           Named
        #               object
        #
        cls = super().__new__(ABCMeta, name, bases, namespace, **kwargs)
        #
        # I use ABCMeta instead of mcls = Workspace
        # in order to avoid toxic inheritance Workspace(ILocalWorkspace)
        # which destroys class variable cls.workspaces
        # 
        try:
            workspaces = set(cls.workspaces)
        except AttributeError:
            workspaces = set()
        #
        # However, the tests are supposed to check that cls is an instance of ILocalWorkspace
        # So I recreate cls
        # 
        if ILocalWorkspace not in bases:
            bases += (ILocalWorkspace,)
        #
        cls = super().__new__(mcls, name, bases, namespace, **kwargs)


        cls_dict = {s: t for s, t in cls.__dict__.items() if not s.startswith('__')}

        tasks_to_replace = {
            s: ProxyTask(s, t)
            for s, t in cls_dict.items() 
            if not callable(t) and isinstance(t, Task)
        }

        for s, t in tasks_to_replace.items():
            setattr(cls, s, t)
            cls_dict[s] = t

        for s, t in cls_dict.items():
            if isinstance(t, Task):
                t._stem_workspace = cls 
                # All tasks (ProxyTask attributes and task-methods) 
                # must have the attribute _stem_workspace 
                # which contains this workspace. -- quote from the assignment

                # here we could have
                # obj_dict[s] = t and
                # setattr(obj, s, t)
                # but `t` was a reference

        tasks_to_show = {
            s: t 
            for s, t in cls_dict.items()
            if isinstance(t, Task)
        }


        cls._tasks = tasks_to_show      # @property .tasks = {return ._tasks} inherited from ILocalWorkspace
        cls._workspaces = workspaces    # @property .workspaces — also inherited from ILocalWorkspace
        cls._name = name                # @property .name — inherited from Named
        #
        # .tasks and .workspaces are mentioned in the assignment,
        # .name is tested in tests (and isn't mentioned in the assignment)


        return cls