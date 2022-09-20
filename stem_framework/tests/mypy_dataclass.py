import dataclasses

from stem.core import Dataclass


@dataclasses.dataclass
class Example:
    a: int = 0
    b: float = 0.0
    c: list = dataclasses.field(default_factory=list)


def function(obj: Dataclass):
    pass


if __name__ == '__main__':
    function(Example())
    function(list())