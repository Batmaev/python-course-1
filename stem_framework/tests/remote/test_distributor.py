import logging
from socketserver import TCPServer
from threading import Thread
import socket
from unittest import TestCase

from stem.envelope import Envelope
from stem.meta import get_meta_attr
from stem.remote.distributor import start_distributor_in_subprocess
from stem.remote.unit import start_unit_in_subprocess
from tests.example_workspace import IntWorkspace

HOST = "localhost"
PORT = 9811

logging.basicConfig(format='%(message)s')
logging.root.setLevel(logging.DEBUG)


class TestDistributor(TestCase):

    def setUp(self) -> None:
        self.servers: list[TCPServer] = []
        self.threads: list[Thread] = []
        self.total_powerfullity = 0
        for i in range(1, 4):
            port = PORT+i
            thread, server = start_unit_in_subprocess(IntWorkspace, HOST, port, i)
            self.threads.append(thread)
            self.servers.append(server)
            self.total_powerfullity += i

        self.distributor_thread, self.distributor_server = start_distributor_in_subprocess(HOST, PORT, self.servers)

    def _send(self, envelope: Envelope):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))
            logging.debug(f'Client sends: {envelope.meta}')
            sock.sendall(envelope.to_bytes())
            response = sock.recv(1024)
            return response

    def test_powerfullity(self):
        for i in range(5):
            response = self._send(Envelope({'command': 'powerfullity'}))
            envelope = Envelope.from_bytes(response)
            meta = envelope.meta
            if get_meta_attr(meta, "status") == "success":
                self.assertEqual(get_meta_attr(meta, "powerfullity"), self.total_powerfullity)
            elif get_meta_attr(meta, "status") == "failed":
                print(get_meta_attr(meta, "error"))

    def tearDown(self) -> None:
        for thread, server in zip(self.threads, self.servers):
            server.shutdown()
            server.server_close()
            thread.join()

        self._send(Envelope({'command': 'stop'}))   # или так:
        # self.distributor_server.get_loop().call_soon_threadsafe(self.distributor_server.close)
        self.distributor_thread.join()
        logging.debug('joined')
