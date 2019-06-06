import unittest

from mock import MagicMock

from dd.api.workflow.actions import Action
from dd.api.workflow.dataset import DatasetTransformation
from dd.api.workflow.sql import SQLOperator


class TestSQLWorkflow(unittest.TestCase):
    def test_SQLOperator_should_have_query_and_execute_methods(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        sql = SQLOperator(context, dataset)

        # Check
        self.assertTrue(hasattr(sql, 'query'))
        self.assertTrue(hasattr(sql, 'execute'))

    def test_query_should_return_DatasetTransformation(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        sql = SQLOperator(context, dataset)

        # When
        result = sql.query("SELECT * FROM foo.bar")

        # Check
        self.assertIsInstance(result, DatasetTransformation)
        self.assertIs(result.predecessors[0], dataset)

    def test_execute_should_return_action(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        sql = SQLOperator(context, dataset)

        # When
        result = sql.execute("SELECT * FROM foo.bar")

        # Check
        self.assertIsInstance(result, Action)
        self.assertIs(result.predecessors[0], dataset)
