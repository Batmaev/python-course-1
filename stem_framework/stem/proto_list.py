from typing import Type, Iterable, Sized, Iterator, NewType
from google.protobuf.reflection import GeneratedProtocolMessageType


class ProtoList(Sized, Iterable):

    def __init__(self, path, proto_class: GeneratedProtocolMessageType):
        self.path = path
        self.proto_class = proto_class

    def __enter__(self) -> "ProtoList":
        self.file = open(self.path, 'rb')

        self.message_lengths = []
        self.message_positions = []

        while (N := self.file.read(8)) != b'':
            self.message_positions.append(self.file.tell())
            N = int.from_bytes(N)
            self.message_lengths.append(N)
            self.file.seek(N, 1)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.__exit__(exc_type, exc_val, exc_tb)

    def __len__(self) -> int:
        return self.message_lengths.__len__()

    def __getitem__(self, item):
        self.file.seek(self.message_positions[item])
        N = self.message_lengths[item]
        return self.proto_class().ParseFromString(self.file.read(N))

    def __iter__(self) -> Iterator[GeneratedProtocolMessageType]:
        for n in range(self.__len__()):
            yield self[n]





