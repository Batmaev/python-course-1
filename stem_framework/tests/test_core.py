from unittest import TestCase

from stem.core import pascal_case_to_snake_case, Named


class CoreTest(TestCase):
    def test_camel_case_to_snake_case(self):
        for source, excepted in zip(["MyTask", "NPYTask"], ["my_task", "npy_task"]):
            with self.subTest(source=source):
                self.assertEqual(pascal_case_to_snake_case(source), excepted)

    def test_named(self):

        class NPYTask(Named):
            pass

        self.assertEqual(NPYTask().name, "npy_task")

        class MyTask(Named):
            _name = "rose"

        self.assertEqual(MyTask().name, "rose")