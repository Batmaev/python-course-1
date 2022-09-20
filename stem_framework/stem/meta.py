"""Metadata is data that provides information about other data.

Alexander Nozik in his pet-project 'DataForge' sees metadata as a user-defined tree of values.

Based on the metadata and with the help of the metadata processor, Mr. Nozik decides what to do.
"""

from dataclasses import dataclass
from typing import Optional, Any, Union


Meta = ... # TODO()

SpecificationField = ... # TODO()

Specification = ... # TODO()


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
    ...
    # TODO()


def update_meta(meta: Meta, **kwargs):
    ...
    # TODO()
