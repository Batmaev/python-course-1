import array
import mmap
from asyncio import StreamReader, StreamWriter
from io import BufferedRWPair, RawIOBase, BufferedReader, BytesIO
from json import JSONEncoder
from typing import Union, Dict, BinaryIO
from dataclasses import is_dataclass, asdict
import json
from .meta import Meta


Binary = Union[bytes, bytearray, memoryview, mmap.mmap] # Стёр array.array, поскольку непонятно, 
                                                        # как скастовать его к T@TaskResult


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
    def read(input: BufferedReader | BytesIO | BinaryIO | BufferedRWPair) -> "Envelope":
        assert b'#~'   == input.read(2), "Envelope header doesn't start with b'#~'"
        assert b'DF02' == input.read(4), "Envelope type (version) is not DF02"
        input.read(2) # MetaType: XML or YAML

        metaLength = int.from_bytes(input.read(4))
        dataLength = int.from_bytes(input.read(4))

        assert b'~#\r\n' == input.read(4), r"Envelope header doesn't end with b'~#\r\n'"

        meta = json.loads(input.read(metaLength))

        if dataLength < Envelope._MAX_SIZE:
            data = input.read(dataLength)
        else:
            data = mmap.mmap(input.fileno(), dataLength, offset = input.tell())
            # will raise an exception if 'input' is not a file and dataLength > _MAX_SIZE

        return Envelope(meta, data)


    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        return Envelope.read(BytesIO(buffer))


    def to_bytes(self) -> bytes:
        output = BytesIO()
        self.write_to(output)
        output.seek(0)
        return output.read()


    def write_to(self, output: RawIOBase | BytesIO | BinaryIO | BufferedRWPair):
        output.write(b'#~')
        output.write(b'DF02')
        output.write(b'..')
        meta_str = bytes(json.dumps(self.meta), 'utf8')

        output.write(len(meta_str ).to_bytes(4))
        output.write(len(self.data).to_bytes(4))
        output.write(b'~#\r\n')

        output.write(meta_str)
        if isinstance(output, BinaryIO):
            output.write(bytes(self.data))
        else:
            output.write(self.data)


    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        await reader.read(2)

    async def async_write_to(self, writer: StreamWriter):
        pass  # TODO(Assignment 11)
