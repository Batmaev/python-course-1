from typing import Optional, Any, Protocol


def pascal_case_to_snake_case(name: str) -> str:
    ... # TODO()



class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        ... # TODO()


class Dataclass(Protocol):
    __dataclass_fields__: Any
    #
    # В Cython есть функция dataclasses.is_dataclass, 
    # которая проверяет наличие поля __dataclass_fields__
    # https://github.com/python/cpython/blob/3.10/Lib/dataclasses.py
    #
    # Мой протокол тоже проверяет наличие этого поля.