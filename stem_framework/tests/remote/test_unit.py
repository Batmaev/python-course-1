import io
import logging
import socket
import time
from unittest import TestCase

from stem.envelope import Envelope
from stem.remote.unit import start_unit_in_subprocess, start_unit
from tests.example_workspace import IntWorkspace

POWERFULLITY = 5
HOST = "localhost"
PORT = 9801
logging.root.setLevel(logging.DEBUG)


class ServerUnitTest(TestCase):
    def test_start_unit(self):
        start_unit(IntWorkspace, HOST, PORT, POWERFULLITY)



class UnitHandlerTest(TestCase):

    def setUp(self) -> None:
        self.process = start_unit_in_subprocess(IntWorkspace, HOST, PORT, POWERFULLITY)
        time.sleep(3.0) # Wait start server

    def _send(self, envelope: Envelope):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))
            sock.sendall(envelope.to_bytes())
            response = sock.recv(1024)
            return response

    def test_powerfullity(self):
        for i in range(5):
            response = self._send(Envelope(dict(command="powerfullity")))
            envelope = Envelope.from_bytes(response)
            self.assertEqual(envelope.meta["powerfullity"], POWERFULLITY)

    def tearDown(self) -> None:
        self._send(Envelope(dict(command="stop")))
        self.process.join()
