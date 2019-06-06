# coding: utf-8
import unittest

from dd.api.workflow.utils import normalize_columns


class TestUtils(unittest.TestCase):
    def test_normalize_columns_should_return_normalized_columns(self):
        # Given
        columns = ["Foo", "Bar    ", "bazé", "foo bar", "foo/bar"]

        # When
        normalized_columns = normalize_columns(columns)

        # Check
        self.assertSequenceEqual(normalized_columns,
                                 ["foo", "bar", "baze", "foo_bar", "foobar"])
