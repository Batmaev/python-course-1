import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from stem.envelope import Envelope
from multiprocessing import Process


class Distributor:
    server = None

    def __init__(self, servers):
        self.servers = servers

    async def __call__(self, reader: StreamReader, writer: StreamWriter):
        pass  # TODO(Assigment 11)


async def start_distributor(host: str, port: int, servers: list[tuple[str, int]]):
    pass  # TODO(Assigment 11)


def start_distributor_in_subprocess(host: str, port: int, servers: list[tuple[str, int]]) -> Process:
    pass  # TODO(Assigment 11)
