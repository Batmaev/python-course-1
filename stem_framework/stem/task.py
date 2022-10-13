from typing import TypeVar, Union, Tuple, Callable, Optional, Generic, Any, Iterator
from abc import ABC, abstractmethod
from .core import Named
from .meta import Specification, Meta
from functools import reduce

T = TypeVar("T")


class Task(ABC, Generic[T], Named):
    dependencies: Tuple[Union[str, "Task"], ...]
    specification: Optional[Specification] = None
    settings: Optional[Meta] = None

    def check_by_meta(self, meta: Meta):
        pass

    @abstractmethod
    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        pass


class FunctionTask(Task[T]):
    def __init__(self, name: str, func: Callable, dependencies: Tuple[str | Task, ...], specification: Specification | None = None, settings: Meta | None = None):
        self._name = name
        self._func = func
        self.dependencies = dependencies
        self.specification = specification
        self.settings = settings
        self.__module__ = func.__module__ # this is needed for 
                                          # IWorkspace.find_default_workspace

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._func(meta, **kwargs)


class DataTask(Task[T]):
    dependencies = ()

    @abstractmethod
    def data(self, meta: Meta) -> T:
        pass

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self.data(meta)


class FunctionDataTask(DataTask[T]):
    def __init__(self, name: str, func: Callable,
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def data(self, meta: Meta) -> T:
        return self._func(meta)


def data(func: Callable[[Meta], T] | None = None, specification: Specification | None = None, **settings) -> FunctionDataTask[T]:
    if func is not None:
        return FunctionDataTask(func.__name__, func, specification, **settings)
    else:
        return lambda func : data(func, specification, **settings)



def task(func: Callable[..., T] | None = None, specification: Optional[Specification] = None, **settings) -> FunctionTask[T] | Callable[[Callable[..., T]], FunctionTask[T]]:
    if func is not None:
        arg_names_without_meta = tuple(arg for arg in func.__code__.co_varnames if arg != 'meta')
        return FunctionTask(func.__name__, func, arg_names_without_meta, specification, **settings)
    else:
        return lambda func : task(func, specification, **settings)


class MapTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence : Union[str, "Task"]):

        self.func = func

        if isinstance(dependence, str):
            self.dependence_name = dependence
        else:
            self.dependence_name = dependence.name

        # Нужно реализовать свойство name.
        # По какой-то нелепой причине MapTask наследуется от Task,
        # а Task наследуется от Named,
        # в котором name определено как @property,
        # которое возвращает свойство ._name
        # Поэтому следующий костыль:
        self._name = 'map_' + self.dependence_name

    def transform(self, meta: Meta, /, **kwargs: Any):
        # судя по тестам kwargs[dependance_name] это итератор
        return map(self.func, kwargs[self.dependence_name])



class FilterTask(Task[Iterator[T]]):
    def __init__(self, key: Callable, dependence: Union[str, "Task"]):
        self.key = key
        
        if isinstance(dependence, str):
            self.dependence_name = dependence
        else:
            self.dependence_name = dependence.name

        self._name = 'filter_' + self.dependence_name

    def transform(self, meta: Meta, /, **kwargs: Any):
        return filter(self.key, kwargs[self.dependence_name])


class ReduceTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self.func = func
        
        if isinstance(dependence, str):
            self.dependence_name = dependence
        else:
            self.dependence_name = dependence.name

        self._name = 'reduce_' + self.dependence_name

    def transform(self, meta: Meta, /, **kwargs: Any):
        return reduce(self.func, kwargs[self.dependence_name])
