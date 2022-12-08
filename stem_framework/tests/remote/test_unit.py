import socket
from time import sleep
from unittest import TestCase

from stem.envelope import Envelope
from stem.meta import get_meta_attr
from stem.remote.unit import start_unit_in_subprocess, start_unit
from tests.example_workspace import IntWorkspace

POWERFULLITY = 5
HOST = "localhost"
PORT = 9801

import logging
logging.basicConfig(format='%(message)s')
logging.root.setLevel(logging.DEBUG)


class ServerUnitTest(TestCase):
    def test_start_unit(self):
        server = start_unit(IntWorkspace, HOST, PORT, POWERFULLITY)
        server.server_close()



class UnitHandlerTest(TestCase):

    def setUp(self) -> None:
        self.process, self.server = start_unit_in_subprocess(IntWorkspace, HOST, PORT, POWERFULLITY)
        sleep(3.0) # Wait start server
        logging.debug('setup completed')

    def _send(self, envelope: Envelope):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))
            sock.sendall(envelope.to_bytes())
            response = sock.recv(1024)
            logging.info(f'client receives: {response}')
            return response

    def test_powerfullity(self):
        for i in range(5):
            response = self._send(Envelope(dict(command="powerfullity")))
            envelope = Envelope.from_bytes(response)
            self.assertEqual(get_meta_attr(envelope.meta, "powerfullity"), POWERFULLITY)

    def tearDown(self) -> None:
        # self._send(Envelope(dict(command="stop")))
        self.server.shutdown()
        self.server.server_close()
        self.process.join()
