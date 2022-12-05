from types import ModuleType
from typing import Dict, Optional, Any, Set, Type, TypeVar, Union
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


class IWorkspace:

    tasks: dict[str, Task] = NotImplemented

    workspaces: set["IWorkspace"] = NotImplemented

    name: str = NotImplemented

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
    def find_default_workspace(task: Task) -> Type["IWorkspace"]:
        if hasattr(task, '_stem_workspace') and task._stem_workspace != NotImplemented:
            return task._stem_workspace
        else:
            module = import_module(task.__module__)
            return IWorkspace.module_workspace(module)

    @staticmethod
    def module_workspace(module: ModuleType) -> Type["IWorkspace"]:
        try:
            return module.__stem_workspace

        except AttributeError:

            tasks = {}
            workspaces = set()

            for s in dir(module):
                t = getattr(module, s)
                if isinstance(t, Task):
                    tasks[s] = t
                if isinstance(t, IWorkspace) or isinstance(t, type) and issubclass(t, IWorkspace):
                    workspaces.add(t)

            module.__stem_workspace = create_workspace(  # type: ignore
                module.__name__, tasks, workspaces
            )

            return module.__stem_workspace


def create_workspace(
        name: str, tasks: Dict[str, Task] = {},
        workspaces: Set[Type["IWorkspace"]] = set()
    ) -> Type["IWorkspace"]:

    return type(name, (IWorkspace,), {
        'name': name, 'tasks': tasks, 'workspaces': workspaces
    })
    # name, tasks and workspaces become class variables (not object fields),
    # thus we can use them in .find_task and .has_task


# I don't need classes Local and ILocal Workspace
#
# They are not valid workspaces
# because .tasks and .workspaces
# must be not @properties, but class variables
#
# class ILocalWorkspace(IWorkspace):

#     @property
#     def tasks(self) -> dict[str, Task]:
#         return self._tasks

#     @property
#     def workspaces(self) -> set["IWorkspace"]:
#         return self._workspaces


# class LocalWorkspace(ILocalWorkspace):

#     def __init__(self, name, tasks=(), workspaces=()):
#         self._name = name
#         self._tasks = tasks
#         self._workspaces = workspaces


class Workspace(type, IWorkspace):
    def __new__(mcls: type["Workspace"], name: str, bases: tuple[type, ...], namespace: dict[str, Any], **kwargs: Any) -> Type[IWorkspace]:

        # otherwise for some reason IWorkspace.@classmethods
        # will use cls = Workspace instead of cls = userclass
        if IWorkspace not in bases:
            bases += (IWorkspace,)

        cls: Type[IWorkspace] = super().__new__(mcls, name, bases, namespace, **kwargs)  # type: ignore
        #
        # Method Resolution Order:
        #
        # Workspace             # calls super().__new__
        #    type               # __new__ is executed and super()-sequence stops
        #    IWorkspace
        #        object

        cls.name = name

        try:
            cls.workspaces = set(cls.workspaces)
        except TypeError:
            cls.workspaces = set()

        for s, t in cls.__dict__.items():
            if isinstance(t, Task):
                # if callable(t):
                #     t = ProxyTask(s, t)  # Task-methods are required to be proxied (but they cannot exist,
                #     setattr(cls, s, t)   # because we redefine __new__ and and no objects can be created)
                t._stem_workspace = cls  # Tasks are required to have reference to the Workspace

        cls.tasks = {
            s: t
            for s, t in cls.__dict__.items()
            if isinstance(t, Task)
        }

        def __new(userclass, *args, **kwargs):
            return userclass

        cls.__new__ = __new  # type: ignore
            # The class-object itself 
            # must be returned on constructor call 
            # of user classes.     -- quote from the assignment

        return cls