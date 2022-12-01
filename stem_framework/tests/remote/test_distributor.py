import logging
import time
import socket
from unittest import TestCase

from stem.envelope import Envelope
from stem.remote.distributor import start_distributor_in_subprocess
from stem.remote.unit import start_unit_in_subprocess, Commands
from tests.example_workspace import IntWorkspace

HOST = "localhost"
PORT = 9811
logging.root.setLevel(logging.DEBUG)


class TestDistributor(TestCase):

    def setUp(self) -> None:
        servers = []
        self.units = []
        self.powerfullity = 0
        for i in range(1, 4):
            port = PORT+i
            unit = start_unit_in_subprocess(IntWorkspace, HOST, port, i)
            self.units.append(unit)
            servers.append((HOST, port))
            self.powerfullity += i

        self.process = start_distributor_in_subprocess(HOST, PORT, servers=servers)
        time.sleep(3) # Wait start servers

    def _send(self, envelope: Envelope):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))
            sock.sendall(envelope.to_bytes())
            response = sock.recv(1024)
            return response

    def test_powerfullity(self):
        for i in range(5):
            response = self._send(Commands.powerfullity)
            envelope = Envelope.from_bytes(response)
            meta = envelope.meta
            if meta["status"] == "success":
                self.assertEqual(meta["powerfullity"], self.powerfullity)
            elif meta["status"] == "failed":
                print(meta["error"])

    def tearDown(self) -> None:
        self._send(Envelope(dict(command="stop")))
        for unit in self.units:
            unit.terminate()
        self.process.join()
