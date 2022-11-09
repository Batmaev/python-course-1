import array
import mmap
from asyncio import StreamReader, StreamWriter
from io import RawIOBase, BufferedReader, BytesIO
from json import JSONEncoder
from typing import Optional, Union, Dict
from dataclasses import is_dataclass, asdict
import json
from .meta import Meta


Binary = Union[bytes, bytearray, memoryview, array.array, mmap.mmap]


class MetaEncoder(JSONEncoder):

    def default(self, obj: Meta) -> Dict:
        if is_dataclass(obj):
            return asdict(obj)
        elif isinstance(obj, dict):
            return obj
        else:
            raise TypeError



class Envelope:
    _MAX_SIZE = 128*1024*1024 # 128 Mb

    def __init__(self, meta: Meta, data : Binary = b''):
        self.meta = meta
        self.data = data

    def __str__(self):
        return str(self.meta)

    @staticmethod
    def read(input: BufferedReader | BytesIO) -> "Envelope":
        assert b'#~'   == input.read(2), "Envelope byte sequence doesn't start with b'#~'"
        assert b'DF02' == input.read(4), 'Envelope type (version) is not DF02'
        input.read(2) # MetaType: XML or YAML

        metaLength = int.from_bytes(input.read(4))
        dataLength = int.from_bytes(input.read(4))

        meta = json.loads(input.read(metaLength))

        if dataLength < Envelope._MAX_SIZE:
            data = input.read(dataLength)
        else:
            data = mmap.mmap(input.fileno(), dataLength, offset = input.tell())
            # will raise an exception if 'input' is BytesIO and dataLength > _MAX_SIZE

        assert b'~#' == input.read(2), "Envelope byte sequence doesn't end with b'~#'"

        return Envelope(meta, data)


    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        return Envelope.read(BytesIO(buffer))


    def to_bytes(self) -> bytes:
        output = BytesIO()
        self.write_to(output)
        output.seek(0)
        return output.read()


    def write_to(self, output: RawIOBase | BytesIO):
        output.write(b'#~')
        output.write(b'DF02')
        output.write(b'..')
        meta_str = bytes(json.dumps(self.meta), 'utf8')

        output.write(len(meta_str ).to_bytes(4))
        output.write(len(self.data).to_bytes(4))
        output.write(meta_str)
        output.write(self.data)

        output.write(b'~#')


    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        pass  # TODO(Assignment 11)

    async def async_write_to(self, writer: StreamWriter):
        pass  # TODO(Assignment 11)
