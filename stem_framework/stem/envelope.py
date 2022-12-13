from math import ceil
import mmap
from tempfile import TemporaryFile
from asyncio import StreamReader, StreamWriter
from io import BufferedRWPair, RawIOBase, BufferedReader, BytesIO, UnsupportedOperation
from json import JSONEncoder
from typing import IO, Union, Dict, BinaryIO
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

    def __init__(self, meta: Meta, data: Binary = b'', tmp_file: IO | None = None):
        self.meta = meta
        self.data = data
        self.tmp_file = tmp_file

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
        tmp_file = None

        if dataLength < Envelope._MAX_SIZE:
            data = input.read(dataLength)
        else:
            try:
                data = mmap.mmap(input.fileno(), dataLength, offset = input.tell())
                # an exception will mean that input doesn't support random access
                # and we will need to create a tmp file
            except UnsupportedOperation:
                tmp_file = TemporaryFile('rwb')
                # write input to tmp by chuncks:
                for _ in range(ceil(dataLength / Envelope._MAX_SIZE)):
                    tmp_file.write(input.read(Envelope._MAX_SIZE))
                tmp_file.flush()

                data = mmap.mmap(tmp_file.fileno(), dataLength)

        return Envelope(meta, data, tmp_file)


    def __del__(self):
        if self.tmp_file is not None:
            self.tmp_file.close()
            # The tempfile library ensures that the file will be removed


    @staticmethod
    def from_bytes(buffer: bytes) -> "Envelope":
        return Envelope.read(BytesIO(buffer))


    def to_bytes(self) -> bytes:
        output = BytesIO()
        self.write_to(output)
        output.seek(0)
        return output.read()


    def write_to(self, output: RawIOBase | BytesIO | BinaryIO | BufferedRWPair | StreamWriter):
        output.write(b'#~')
        output.write(b'DF02')
        output.write(b'..')
        meta_str = bytes(json.dumps(self.meta), 'utf8')

        output.write(len(meta_str ).to_bytes(4))
        output.write(len(self.data).to_bytes(4))
        output.write(b'~#\r\n')

        output.write(meta_str)
        if isinstance(output, BinaryIO | StreamWriter):
            output.write(self.data[:])  # hope it doesn't load the entire file into memory
        else:                           # otherwise we need to write in chuncks
            output.write(self.data)


    @staticmethod
    async def async_read(reader: StreamReader) -> "Envelope":
        assert b'#~'   == await reader.read(2), "Envelope header doesn't start with b'#~'"
        assert b'DF02' == await reader.read(4), "Envelope type (version) is not DF02"
        await reader.read(2) # MetaType: XML or YAML

        metaLength = int.from_bytes(await reader.read(4))
        dataLength = int.from_bytes(await reader.read(4))

        assert b'~#\r\n' == await reader.read(4), r"Envelope header doesn't end with b'~#\r\n'"

        meta = json.loads(await reader.read(metaLength))
        tmp_file = None

        if dataLength < Envelope._MAX_SIZE:
            data = await reader.read(dataLength)
        else:
            tmp_file = TemporaryFile('rwb')
            # write input to tmp in chuncks:
            for _ in range(ceil(dataLength / Envelope._MAX_SIZE)):
                tmp_file.write(await reader.read(Envelope._MAX_SIZE))
            tmp_file.flush()

            data = mmap.mmap(tmp_file.fileno(), dataLength)

        return Envelope(meta, data, tmp_file)


    async def async_write_to(self, writer: StreamWriter):
        self.write_to(writer)