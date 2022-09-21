"""Metadata is data that provides information about other data.

Alexander Nozik in his pet-project 'DataForge' sees metadata as a user-defined tree of values.

Based on the metadata and with the help of the metadata processor, Mr. Nozik decides what to do.
"""

from dataclasses import dataclass, is_dataclass
from typing import Optional, Any, Tuple, Type, Union

from stem.core import Dataclass


Meta = dict | Dataclass

SpecificationField = Tuple[
    object,                                         # Key
    Type | Tuple[Type, ...] | 'SpecificationField'  # Value
]
"""You must use `object` instead of `Any` in type specifications,
because isinstance(var, Any), issubclass(float, Any) don't work.
Also type(Any) != type."""

Specification = Dataclass | Tuple[SpecificationField, ...]


class SpecificationError(Exception):
    pass


@dataclass
class MetaFieldError:
    required_key: str
    required_types: Type | Tuple[Type, ...] | None = None
    presented_type: Type | None = None
    presented_value: Any = None


class MetaVerification:

    def __init__(self, *errors: Union[MetaFieldError, "MetaVerification"]):
        self.error = errors
        # why not self.errors?

        self.checked_success = errors == ()


    @staticmethod
    def verify(meta: Meta,
               specification: Optional[Specification] = None) -> "MetaVerification":

        if is_dataclass(meta):
            meta_keys = meta.__dataclass_fields__.keys()
        else: 
            # meta is dict
            meta_keys = meta.keys()

        if is_dataclass(specification):
            specification_keys = specification.__dataclass_fields__.keys()
        else:
            # specification was tuple of pairs
            specification = dict(specification)
            specification_keys = specification.keys()

        errors = []
        for required_key in specification_keys:
            if is_dataclass(specification):
                required_types = specification.__dataclass_fields__[required_key].type
            else:
                # specification was tuple of pairs of types and now is dict
                required_types = specification[required_key]

            if required_key not in meta_keys:
                errors.append(
                    MetaFieldError(
                        required_key = required_key,
                        required_types = required_types
                    )
                )
            else:
                presented_value = get_meta_attr(meta, required_key)
                presented_type = type(presented_value)

                if (isinstance(required_types, type) or (
                    isinstance(required_types, tuple) and isinstance(required_types[0], type)
                )):
                    # Выход из рекурсии
                    if not issubclass(presented_type, required_types):
                        errors.append(
                            MetaFieldError(
                                required_key = required_key,
                                required_types = required_types,
                                presented_value = presented_value,
                                presented_type = presented_type
                            )
                        )
                else:
                    # Вход в рекурсию
                    errors_next_level = MetaVerification.verify(
                        get_meta_attr(meta, required_key),
                        required_types
                    ).error

                    if errors_next_level != ():
                        errors.append(errors_next_level)

        return MetaVerification(*errors)


def get_meta_attr(meta : Meta, key : str, default : Optional[Any] = None) -> Optional[Any]:

    # maybe 'meta' is dict:
    try:
        return meta[key]
    except KeyError:
        # 'meta' is dict but doesn't have required field
        return default
    except TypeError:
        # 'meta' is not a dict => we will check next option
        pass

    # maybe 'meta' is dataclass:
    try:
        return getattr(meta, key)
    except AttributeError:
        # meta probably is a dataclass but doesn't have the required field
        return default



def update_meta(meta: Meta, **kwargs):
    """Decides whether 'meta' is a dataclass or a dict
    and then updates existing fields or cretes new ones.
    
    ```python
    a = {'position': 0}
    update_meta(a, position = 1, velocity = 2)
    print(a)    # {'position': 1, 'velocity': 2}
    ```

    For a dataclass instance, if the field does not exist, a new **hidden** field will be created, which will not be added to `meta.__dataclass_fields__ `and will not be shown in the output of `__repr__()`

    ```python
    @dataclass
    class Data:
        position: float
    
    my_data = Data(0.0)
    update_meta(my_data, position = 1.0, velocity = 2.0)

    print(my_data)          # Data(position=1.0)
    print(my_data.velocity) # 2.0
    ```

    The field `my_data.velocity` was created but `__repr__()` wasn't aware.
    """

    if is_dataclass(meta):
        for k, v in kwargs.items():
            setattr(meta, k, v)
    else:
        # then it should be a dict
        for k, v in kwargs.items():
            meta[k] = v
