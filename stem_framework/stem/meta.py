"""Metadata is data that provides information about other data.

Alexander Nozik in his pet-project 'DataForge' sees metadata as a user-defined tree of values.

Based on the metadata and with the help of the metadata processor, Mr. Nozik decides what to do.
"""

from dataclasses import dataclass
from typing import Optional, Any, Tuple, Type, Union

from stem.core import Dataclass


Meta = dict | Dataclass

SpecificationField = Tuple[
    Any,                                            # Key
    Type | Tuple[Type, ...] | 'SpecificationField'  # Value
]

Specification = Dataclass | Tuple[SpecificationField, ...]


class SpecificationError(Exception):
    pass


@dataclass
class MetaFieldError:
    required_key: str
    required_types: Optional[tuple[type]] = None
    presented_type: Optional[type] = None
    presented_value: Any = None


class MetaVerification:

    def __init__(self, *errors: Union[MetaFieldError, "MetaVerification"]):
        self.error = errors
        # TODO("checked_success")

    @staticmethod
    def verify(meta: Meta,
               specification: Optional[Specification] = None) -> "MetaVerification":
        ...  # TODO()


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
    ...
    # TODO()
