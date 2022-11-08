import io
from unittest import TestCase

from stem.envelope import Envelope


class TestEnvelope(TestCase):

    def setUp(self) -> None:
        self.data = b"0123456789"
        self.envelope = Envelope(dict(a=1, b="b"), self.data)

    def test_read(self):
        data = self.envelope.to_bytes()
        envelope = Envelope.from_bytes(data)
        self.assertDictEqual(self.envelope.meta, envelope.meta)
        self.assertEqual(self.envelope.data, envelope.data)
