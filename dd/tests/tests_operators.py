import unittest

from dd.operators import DDTransformation, LocalOperator
from mock import MagicMock


class TestDDTransformation(unittest.TestCase):

    def test_execute_should_return_output_table_and_set_result_dataframe_with_result(self):
        # given
        operator, table = self.build_dd_transformation()

        # when
        result = operator.execute()

        # then
        self.assertEqual(result, table)
        self.assertEqual(operator.result_dataframe, 'Dataframe')

    def test_get_result_should_return_resulted_dataframe_after_execute_call(self):
        # given
        operator, _ = self.build_dd_transformation()
        operator.execute()

        # when
        result = operator.get_result()

        # then
        self.assertEqual(result, 'Dataframe')

    def test_get_result_should_return_None_by_default(self):
        # given
        operator, _ = self.build_dd_transformation()

        # when
        result = operator.get_result()

        # then
        self.assertIsNone(result)

    def build_dd_transformation(self):
        transformation_func = lambda: 'Dataframe'
        table = 'output_table'
        task_id = '0'
        operator = DDTransformation(output_table=table, python_callable=transformation_func, task_id=task_id)
        return operator, table


class TestLocalOperator(unittest.TestCase):

    def build_local_operator(self):
        context = MagicMock()
        context.increment_task_id_counter.return_value = 0
        table = 'output_table'
        task_id = '0'
        transformation_func = lambda: 'Dataframe'
        operator = LocalOperator(python_callable=transformation_func, task_id=task_id, output_table=table)
        return operator, table

    def test_execute_should_return_output_table_and_set_result_dataframe_with_result(self):
        # given
        operator, table = self.build_local_operator()

        # when
        result = operator.execute()

        # then
        self.assertEqual(result, table)
        self.assertEqual(operator.result_dataframe, 'Dataframe')

    def test_execute_should_return_output_table_and_set_result_dataframe_with_result(self):
        # given
        operator, table = self.build_local_operator()

        # when
        result = operator.execute()

        # then
        self.assertEqual(result, table)
        self.assertEqual(operator.result_dataframe, 'Dataframe')

    def test_get_result_should_return_resulted_dataframe_after_execute_call(self):
        # given
        operator, table = self.build_local_operator()
        operator.execute()

        # when
        result = operator.get_result()

        # then
        self.assertEqual(result, 'Dataframe')

    def test_get_result_should_return_None_by_default(self):
        # given
        operator, table = self.build_local_operator()

        # when
        result = operator.get_result()

        # then
        self.assertIsNone(result)