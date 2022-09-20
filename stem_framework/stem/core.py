from typing import Optional, Any, Protocol
import re


def pascal_case_to_snake_case(name: str) -> str:
    """Вставляет '_' перед каждой заглавной буквой, 
    если после неё идёт маленькая и она не стоит в начале строки"""
    return re.sub(
        r"(?<!^)[A-Z][a-z]",
        r"_\g<0>",
        name
    ).lower()



class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        if self._name is not None:
            return self._name
        else:
            return pascal_case_to_snake_case(
                self.__class__.__name__
            )


class Dataclass(Protocol):
    __dataclass_fields__: Any
    #
    # В Cython есть функция dataclasses.is_dataclass, 
    # которая проверяет наличие поля __dataclass_fields__
    # https://github.com/python/cpython/blob/3.10/Lib/dataclasses.py
    #
    # Мой протокол тоже проверяет наличие этого поля.