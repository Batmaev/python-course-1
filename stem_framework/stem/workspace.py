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

    tasks: dict[str, Task] = NotImplemented

    workspaces: set["IWorkspace"] = NotImplemented

    @classmethod # in tests, it is used as a @classmethod
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
                if (t := w.find_task(task_path)) is not None:
                    return t
            return None

    @classmethod
    def has_task(cls, task_path: Union[str, TaskPath]) -> bool:
        return cls.find_task(task_path) is not None

    @classmethod
    def get_workspace(cls, name) -> Optional["IWorkspace"]:
        for workspace in cls.workspaces:
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
                if isinstance(t, type) and issubclass(t, IWorkspace):
                    workspaces.add(t)

            module.__stem_workspace = create_workspace(
                module.__name__, tasks, workspaces
            )

            return module.__stem_workspace

def create_workspace(name, tasks = {}, workspaces = set()):
    return type(name, (IWorkspace,), {'name': name, 'tasks': tasks, 'workspaces': workspaces})
    # name, tasks and workspaces become class variables (not object fields),
    # thus we can use them in .find_task and .has_task


# I don't need classes Local and ILocal Workspace
class ILocalWorkspace(IWorkspace):

    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):

    def __init__(self, name, tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta):
    def __new__(mcls: type[Self], name: str, bases: tuple[type, ...], namespace: dict[str, Any], **kwargs: Any) -> Self:

        # adds a lot of methods
        if ILocalWorkspace not in bases:
            bases += (IWorkspace,)

        cls = super().__new__(mcls, name, bases, namespace, **kwargs)
        #
        # Method Resolution Order:
        #
        # Workspace             # calls super().__new__
        #   ABCMeta             # calls super().__new__
        #       type            # __new__ is executed and super()-sequence stops
        #           object

        cls.name = name

        try:
            cls.workspaces = set(cls.workspaces)
        except TypeError:
            cls.workspaces = set()

        for s, t in cls.__dict__.items():
            if isinstance(t, Task):
                if not callable(t):
                    t = ProxyTask(s, t)  # Task-methods are required to be proxied
                    setattr(cls, s, t)
                t._stem_workspace = cls  # Tasks are required to have reference to the Workspace

        cls.tasks = {
            s: t
            for s, t in cls.__dict__.items()
            if isinstance(t, Task)
        }

        def __new(userclass, *args, **kwargs):
            return userclass

        cls.__new__ = __new
            # The class-object itself 
            # must be returned on constructor call 
            # of user classes.     -- quote from the assignment

        return cls