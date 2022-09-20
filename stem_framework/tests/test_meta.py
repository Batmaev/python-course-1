import dataclasses
from unittest import TestCase

from stem.meta import MetaVerification, update_meta, get_meta_attr


@dataclasses.dataclass
class Example:
    a: int = 0
    b: float = 0.0
    c: list = dataclasses.field(default_factory=list)


class CoreTest(TestCase):

    def test_get_meta_attr(self):
        example = Example()
        self.assertEqual(get_meta_attr(example, "a"), example.a)
        self.assertEqual(get_meta_attr(example, "b"), example.b)
        self.assertEqual(get_meta_attr(example, "c"), example.c)
        self.assertEqual(get_meta_attr(example, "d", "D"), "D")

    def test_update_meta(self):
        example = Example()
        update_meta(example, a=1, d="D")
        self.assertEqual(example.a, 1)
        self.assertEqual(example.d, "D")

    def test_verify(self):
        example = Example()
        example_dict = dataclasses.asdict(example)

        specification = (("a", int), ("b", (int, float)))

        verification = MetaVerification.verify(example, specification)
        self.assertTrue(verification.checked_success)

        verification = MetaVerification.verify(example_dict, Example)
        self.assertTrue(verification.checked_success)

        example_dict.pop("a")
        verification = MetaVerification.verify(example_dict, Example)
        self.assertFalse(verification.checked_success)

        verification = MetaVerification.verify(example_dict, specification)
        self.assertFalse(verification.checked_success)
