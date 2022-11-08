import array
import mmap
from asyncio import StreamReader, StreamWriter
from io import RawIOBase, BufferedReader
from json import JSONEncoder
from typing import Optional, Union, Any
from .meta import Meta


Binary = Union[bytes, bytearray, memoryview, array.array, mmap.mmap]


class MetaEncoder(JSONEncoder):

    def default(self, obj: Meta) -> Any:
        pass # TODO(Assignment 7)


class Envelope:
    _MAX_SIZE = 128*1024*1024 # 128 Mb

    def __init__(self, meta: Meta, data : Optional[Binary] = None):
        self.meta = meta
        self.data = data

    def __str__(self):
        return str(self.meta)

    @staticmethod
    def read(input: BufferedReader) -> "Envelope":
        pass # TODO(Assignment 7)

    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        pass  # TODO(Assignment 7)

    def to_bytes(self) -> bytes:
        pass  # TODO(Assignment 7)

    def write_to(self, output: RawIOBase):
        pass  # TODO(Assignment 7)

    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        pass  # TODO(Assignment 11)

    async def async_write_to(self, writer: StreamWriter):
        pass  # TODO(Assignment 11)
