import tempfile
import unittest

import numpy as np
import pandas as pd
from airflow import DAG
from datetime import datetime
from mock import MagicMock, patch

import dd.api.workflow.dataset
from dd import DB
from dd.api.workflow.actions import Action
from dd.api.workflow.sql import SQLOperator

dd.api.workflow.dataset.is_ipython = lambda: True
dd.api.workflow.actions.is_ipython = lambda: True

from dd.api.contexts.distributed import AirflowContext
from dd.api.workflow.dataset import Dataset, DatasetLoad, DatasetTransformation


class TestDataset(unittest.TestCase):
    def setUp(self):
        self.workflow = MagicMock(spec_set=DAG("test_workflow", start_date=datetime.now()))
        self.workflow.dag_id = "mock"
        self.db = MagicMock()
        self.db.query.result_value = None


    def test_creating_dataset_should_add_task_to_workflow(self):
        # Given
        workflow = self.workflow
        db = self.db

        # When
        _ = AirflowContext(workflow, db).create_dataset("table")

        # Assert
        workflow.add_task.assert_called_once()

    def test_apply_method_should_run(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])
        expected_result1 = pd.DataFrame([[np.nan, 7], [6, 7]])

        # With a function with only args
        def my_apply_function(indf, arg1, arg2, arg3):
            self.assertEqual(arg1, 1)
            self.assertEqual(arg2, 2)
            self.assertEqual(arg3, 3)
            odf = indf.applymap(lambda t: t + arg1 + arg2 + arg3)
            self.assertTrue(odf.equals(expected_result1))

        # When a valid execution
        new_action = dataset.apply(my_apply_function, 1, 2, 3)

        # Assert
        self.assertFalse(new_action.executed)
        new_action.execute()

    def test_apply_method_should_raise_when_invalid_number_args(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])

        # With a function with only args
        def my_apply_function(indf, arg1, arg2, arg3):
            pass

        # When
        new_action = dataset.apply(my_apply_function, 1, 2)

        # Assert
        self.assertFalse(new_action.executed)

        with self.assertRaises(TypeError) as context:
            new_action.execute()


        possible_exceptions = ["my_apply_function() missing 1 required positional argument: 'arg3'", # msg Python 3
                               "my_apply_function() takes exactly 4 arguments (3 given)"] # msg Python 2
        self.assertIn(str(context.exception), possible_exceptions)

        # When
        new_action = dataset.apply(my_apply_function)

        # Assert
        self.assertFalse(new_action.executed)

        with self.assertRaises(TypeError) as context:
            new_action.execute()

        possible_exceptions = ["my_apply_function() missing 3 required positional arguments: 'arg1', 'arg2', and 'arg3'", # msg Python 3
                               "my_apply_function() takes exactly 4 arguments (1 given)"] # msg Python 2
        self.assertIn(str(context.exception), possible_exceptions)

    def test_transform_method_should_return_new_dataset(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        new_dataset = dataset.transform(lambda x: x)

        # Assert
        self.assertIsNot(new_dataset, dataset)
        self.assertIsInstance(new_dataset, Dataset)

    def test_transform_method_should_handle_optional_kwargs(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        dataset2 = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])
        expected_result1 = pd.DataFrame([[np.nan, 2], [1, 2]])

        # With a function with only args
        def my_transform_function(indf, df2, arg1=0):
            return indf.applymap(lambda t: t + arg1)

        # When
        new_dataset = dataset.transform(my_transform_function,
                                        arg1=1,
                                        output_table="mytable",
                                        datasets=[dataset2],
                                        write_options=dict(if_exists="replace"))

        # Assert
        self.assertIsNone(new_dataset.dataframe)
        self.assertFalse(new_dataset.executed)

        # Finally
        new_dataset.execute()
        new_dataset.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result1
        ))
        self.assertTrue(new_dataset.output_table == "mytable")

    def test_transform_method_should_raise_when_invalid_number_args(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])
        expected_result1 = pd.DataFrame([[np.nan, 4], [3, 4]])

        # With a function with only args
        def my_transform_function(indf, arg1, arg2, arg3):
            return indf.applymap(lambda t: t + arg1 + arg2 + arg3)

        # When
        new_dataset = dataset.transform(my_transform_function, 1, 2)

        # Assert
        self.assertIsNone(new_dataset.dataframe)
        self.assertFalse(new_dataset.executed)

        with self.assertRaises(TypeError) as context:
            new_dataset.execute()

        possible_exceptions = ["my_transform_function() missing 1 required positional argument: 'arg3'", # msg Python 3
                               "my_transform_function() takes exactly 4 arguments (3 given)"] # msg Python 2
        self.assertIn(str(context.exception), possible_exceptions)

        # When
        new_dataset = dataset.transform(my_transform_function)

        # Assert
        self.assertIsNone(new_dataset.dataframe)
        self.assertFalse(new_dataset.executed)

        with self.assertRaises(TypeError) as context:
            new_dataset.execute()


        possible_exceptions = ["my_transform_function() missing 3 required positional arguments: 'arg1', 'arg2', and 'arg3'", # msg Python 3
                               "my_transform_function() takes exactly 4 arguments (1 given)"] # msg Python 2
        self.assertIn(str(context.exception), possible_exceptions)


        # When
        new_dataset = dataset.transform(my_transform_function, 1, 1, 1)

        # Assert
        self.assertIsNone(new_dataset.dataframe)
        self.assertFalse(new_dataset.executed)

        # Finally
        new_dataset.execute()
        new_dataset.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result1
        ))

    def test_transform_method_should_handle_args_kwargs(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])
        expected_result1 = pd.DataFrame([[np.nan, 2], [1, 2]])
        expected_result2 = pd.DataFrame([[np.nan, 3], [2, 3]])

        # With a function with arg and kwargs
        def mytransfun(indf, myarg1, mynamedarg1=1):
            return indf.applymap(lambda t: t + myarg1 - mynamedarg1)

        # When
        new_dataset = dataset.transform(mytransfun, 2)

        # Assert
        self.assertIsNone(new_dataset.dataframe)

        self.assertFalse(new_dataset.executed)
        new_dataset.execute()
        new_dataset.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result1
        ))

        # When
        new_dataset = dataset.transform(mytransfun, 2, mynamedarg1=0)

        # Assert
        self.assertIsNone(new_dataset.dataframe)

        self.assertFalse(new_dataset.executed)
        new_dataset.execute()
        new_dataset.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result2
        ))

    def test_transform_method_should_apply_function_to_dataset(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        self.db.retrieve_table.return_value = pd.DataFrame([[np.nan, 1],
                                                            [0, 1]])
        dataset2 = context.create_dataset("table")
        expected_result1 = pd.DataFrame([[np.nan, 2], [1, 2]])
        expected_result2 = pd.DataFrame([[0.0, 1], [0.0, 1]])

        # When
        new_dataset = dataset.transform(lambda x: x.applymap(lambda t: t + 1))
        new_dataset2 = dataset2.transform(lambda df: df.fillna(0))

        # Assert
        self.assertIsNone(new_dataset.dataframe)
        self.assertIsNone(new_dataset2.dataframe)

        self.assertFalse(new_dataset.executed)
        self.assertFalse(new_dataset2.executed)
        new_dataset.execute()
        new_dataset.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result1
        ))

        new_dataset2.execute()
        new_dataset2.collect()
        self.assertTrue(self.db.import_dataframe.call_args[0][0].equals(
            expected_result2
        ))

    def test_transform_method_should_be_able_to_process_multiple_datasets(
            self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset1 = context.create_dataset("table")
        dataset2 = context.create_dataset("table")
        mock_function = MagicMock()
        mock_function.__name__ = "mock"
        new_dataset = dataset1.transform(mock_function, datasets=[dataset2])

        # When
        new_dataset.execute()
        new_dataset.collect()

        # Check
        args, kwargs = mock_function.call_args
        self.assertTrue(args[0], dataset1)
        self.assertTrue(args[1], dataset2)

    def test_collect_should_return_dataframe_attribute_when_non_empty(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        initial_dataframe = pd.DataFrame([[0.0, 1], [0.0, 1]])
        dataset.dataframe = initial_dataframe

        # When
        dataframe = dataset.collect()

        # Assert
        self.assertIsInstance(dataframe, pd.DataFrame)
        self.assertTrue(dataframe.equals(initial_dataframe))

    def test_collect_should_call_db_retrieve_table_when_empty(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        output_table = "output_table"
        dataset.output_table = output_table

        # When
        dataset.collect()

        # Assert
        self.db.retrieve_table.assert_called_once_with(output_table)

    def test_split_train_test_should_return_two_datasets(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        train, test = dataset.split_train_test()

        # Assert
        self.assertIsInstance(train, Dataset)
        self.assertIsInstance(test, Dataset)

    def test_join_should_return_new_dataset(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset_left = context.create_dataset("table")
        dataset_right = context.create_dataset("table")

        # When
        join = dataset_left.join(dataset_right)

        # Check
        self.assertIsInstance(join, Dataset)

    def test_execute_should_call_operator_execute_once(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table").transform(lambda x: x)
        dataset.operator = MagicMock()

        # When
        dataset.execute()
        dataset.execute()

        # Check
        dataset.operator.execute.assert_called_once()

    def test_execute_with_force_should_call_operator_execute_twice(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table").transform(lambda x: x)
        dataset.operator = MagicMock()

        # When
        dataset.execute()
        dataset.execute(force=True)

        # Check
        self.assertEqual(dataset.operator.execute.call_count, 2)

    def test_execute_when_operator_is_DDOperator_should_return_resulted_dataframe_from_operator_get_result(self):
        # Given
        dataset = Dataset(MagicMock(), 'output')
        dataset.executed = False
        dataset.operator = MagicMock()
        dataset.operator.execute = lambda: 'output_table'
        dataset.operator.get_result = lambda: 'Dataframe'
        dataset.operator.set_upstream = None

        # When
        result = dataset.execute()

        # Check
        self.assertEqual(result, 'Dataframe')

    def test_transform_with_if_exists_should_append_to_existing_table(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        new_dataset = dataset.transform(lambda x: x,
                                        write_options=dict(if_exists="append"))

        # When
        new_dataset.execute()

        # Check
        self.assertIn("if_exists", self.db.import_dataframe.call_args[1])
        self.assertEqual(self.db.import_dataframe.call_args[1]["if_exists"],
                         "append")

    def test_select_columns_should_create_new_dataset(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        new_dataset = dataset.select_columns(["foo", "bar"])

        # Check
        self.assertIsInstance(new_dataset, Dataset)
        self.assertIsNot(new_dataset, dataset)

    def test_default_is_cached_should_match_context_auto_persistence(self):
        # Given
        persisted_context = MagicMock()
        persisted_context.auto_persistence = True

        unpersisted_context = MagicMock()
        unpersisted_context.auto_persistence = False

        # When
        persisted_dataset = Dataset(persisted_context, "foo")
        unpersisted_dataset = Dataset(unpersisted_context, "bar")

        # Check
        self.assertTrue(persisted_dataset.is_cached)
        self.assertFalse(unpersisted_dataset.is_cached)

    def test_is_cached_attribute_may_be_set_by_cache_method(self):
        # Given
        context = MagicMock()
        context.auto_persistence = False
        dataset = Dataset(context, "foo")

        # When
        dataset.cache()

        # Check
        self.assertTrue(dataset.is_cached)

        # Then when
        dataset.cache(boolean=False)

        # Check
        self.assertFalse(dataset.is_cached)

    def test_memory_usage_returns_integer(self):
        # Given
        context = MagicMock()
        context.auto_persistence = False
        dataset = Dataset(context, "foo")

        # When
        usage = dataset.memory_usage

        # Check
        self.assertIsInstance(usage, int)

    def test_providing_output_table_in_select_columns_must_set_output_table(
            self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        new_dataset = dataset.select_columns(["foo", "bar"],
                                             output_table="myoutput.table")

        # Check
        self.assertEqual(new_dataset.output_table, "myoutput.table")

    def test_sql_query_should_return_dataset(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        new_dataset = dataset.sql.query("SELECT * FROM foo.bar")

        # Check
        self.assertIsInstance(new_dataset, Dataset)

    def test_sql_query_should_call_db_query(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        qw = dataset.sql.query("SELECT * FROM foo.bar")
        qw.execute()  # In airflow context we force execution
        qw.head()

        # Check
        self.db.query.assert_called_once_with("SELECT * FROM foo.bar")

    def test_sql_execute_should_return_action(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        action = dataset.sql.execute("SELECT * FROM foo.bar")

        # Check
        self.assertIsInstance(action, Action)

    def test_sql_execute_should_call_db_execute(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        action = dataset.sql.execute("SELECT * FROM foo.bar")

        # When
        action.execute(force=True)

        # Check
        self.db.execute.assert_called_once_with("SELECT * FROM foo.bar")

    def test_apply_should_return_action(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")
        mock_function = MagicMock()
        mock_function.__name__ = "mock"

        # When
        result = dataset.apply(mock_function)

        # Check
        self.assertIsInstance(result, Action)

    def test_sql_should_be_SQLOperator(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        result = dataset.sql

        # Check
        self.assertIsInstance(result, SQLOperator)

    def test_sql_should_have_same_context(self):
        # Given
        context = AirflowContext(self.workflow, self.db)
        dataset = context.create_dataset("table")

        # When
        result = dataset.sql

        # Check
        self.assertIs(result.context, dataset.context)

    def test_multitransform_method_should_allow_multiple_output_datasets(self):
        # Given
        with tempfile.NamedTemporaryFile() as tmp:
            workflow = DAG("test_workflow", start_date=datetime.now())
            db = DB(dbtype='sqlite', filename=tmp.name)
            ctx = AirflowContext(workflow, db)

            # given
            df = pd.DataFrame([[np.nan, 2], [1, 2]])
            df.columns = map(lambda x: "num_" + str(x), df.columns)
            expected_result2 = pd.DataFrame([[np.nan, 3], [2, 3]])
            expected_result2.columns = map(lambda x: "num_" + str(x), expected_result2.columns)

            db.import_dataframe(df, "test_num", index=False)

            dataset = ctx.table("test_num")

            # when
            def my_multiple_output(indf):
                return indf, indf + 1

            new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])

            # then
            self.assertIsNone(new_df1.dataframe)
            self.assertFalse(new_df1.executed)
            self.assertIsNone(new_df2.dataframe)
            self.assertFalse(new_df2.executed)

            # finally
            new_df1.execute()

            # same result
            odf1 = new_df1.collect()
            odf2 = new_df2.collect()
            pd.testing.assert_frame_equal(odf1, df)
            pd.testing.assert_frame_equal(odf2, expected_result2)

    def test_multitransform_should_handle_column_method(self):
        # Given
        ctx = self._get_airflow_context()
        ctx.db.import_dataframe(pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"]),
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        # then columns must be equal
        new_df1_cols = list(new_df1.columns)
        new_df2_cols = list(new_df2.columns)
        self.assertEqual(new_df1_cols, new_df2_cols)

    def test_multitransform_should_handle_shape_method(self):
        # Given
        ctx = self._get_airflow_context()
        ctx.db.import_dataframe(pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"]),
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        # then shapes must be equal
        new_df1_sh = new_df1.shape
        new_df2_sh = new_df2.shape
        self.assertEqual(new_df1_sh, new_df2_sh)

    def test_multitransform_should_handle_memory_usage_method(self):
        # Given
        ctx = self._get_airflow_context()
        ctx.db.import_dataframe(pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"]),
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        # then memory usage must be equal
        mu1 = new_df1.memory_usage
        mu2 = new_df2.memory_usage
        self.assertEqual(mu1, mu2)

    def test_multitransform_should_handle_head_method(self):
        # Given
        ctx = self._get_airflow_context()
        df = pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"])
        ctx.db.import_dataframe(df,
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        # then head must be equal
        pd.testing.assert_frame_equal(new_df1.head(2), df.head(2))
        pd.testing.assert_frame_equal(new_df2.head(2), df.head(2) + 1)

    def test_multitransform_should_handle_sql_operator(self):
        # Given
        ctx = self._get_airflow_context()
        df = pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"])
        ctx.db.import_dataframe(df,
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        result = ctx.db.read_sql("select * from odf1")

        # then dataframe must be equal
        pd.testing.assert_frame_equal(result, df)

    def test_multitransform_should_handle_join_method(self):
        # Given
        ctx = self._get_airflow_context()
        df = pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"])
        ctx.db.import_dataframe(df,
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        new_df3 = new_df1.join(new_df2, left_index=True, right_index=True)
        result = new_df3.collect()

        # then dataframe must be equal
        pd.testing.assert_frame_equal(result, df.merge(df + 1, left_index=True, right_index=True))

    def test_multitransform_should_handle_select_columns_method(self):
        # Given
        ctx = self._get_airflow_context()
        df = pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"])
        ctx.db.import_dataframe(df,
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        new_df3 = new_df1.select_columns(["n1"])
        result = new_df3.collect()

        # then dataframe must be equal
        pd.testing.assert_frame_equal(result, df[["n1"]])

    def test_multitransform_should_handle_split_train_test_method(self):
        # Given
        ctx = self._get_airflow_context()
        df = pd.DataFrame([[np.nan, 2], [1, 2]], columns=["n1", "n2"])
        ctx.db.import_dataframe(df,
                                "test_num", index=False)
        dataset = ctx.create_dataset("test_num")

        # when
        def my_multiple_output(indf):
            return indf, indf + 1

        new_df1, new_df2 = dataset.multitransform(my_multiple_output, output_tables=["odf1", "odf2"])
        new_df1.execute()

        new_train, new_test = new_df1.split_train_test(0.5, seed=0)

        # then dataframe must be equal
        result = new_train.collect()
        pd.testing.assert_frame_equal(result, df.head(1))

    def _get_airflow_context(self):
        workflow = DAG("test_workflow", start_date=datetime.now())
        db = self._get_testdb()
        ctx = AirflowContext(workflow, db)
        return ctx

    def _get_testdb(self):
        db = DB(dbtype='sqlite', filename=':memory:')
        return db

    def test_head_should_do_nothing_but_logs_info_when_is_ipython_is_False(self):
        # given
        mock_collect = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.task_id_counter.return_value=1
        mock_is_ipython = MagicMock()
        mock_is_ipython.return_value = False
        dataset = Dataset(mock_ctx, None)

        # when
        with patch('dd.api.workflow.dataset.is_ipython', mock_is_ipython):
            with patch('dd.api.workflow.dataset.Dataset.collect', mock_collect):
                dataset.head()

        # then
        mock_is_ipython.assert_called()
        mock_collect.assert_not_called()

    def test_head_should_call_head_when_is_ipython_is_False(self):
        # given
        mock_collect = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.task_id_counter.return_value=1
        mock_is_ipython = MagicMock()
        mock_is_ipython.return_value = False

        # when
        with patch('dd.api.workflow.dataset.is_ipython', mock_is_ipython):
            with patch('dd.api.workflow.dataset.Dataset.collect', mock_collect):
                Dataset(mock_ctx, None).head()

        # then
        mock_is_ipython.assert_called()
        mock_collect.assert_not_called()

    def test_build_operator_should_replace_slash_in_output_table_by_dot(self):
        # given
        mock_airflow_context = AirflowContext(DAG(dag_id='test', start_date=datetime.now()), MagicMock(), uuid=1)
        mock_builder = MagicMock()
        output_table = "la/la/la"

        # when
        dataset = DatasetLoad(mock_airflow_context, mock_builder, output_table)
        result = dataset.operator
        dataset = DatasetTransformation(dataset, mock_airflow_context, mock_builder, output_table)
        result2 = dataset.operator

        # then
        self.assertEqual('la.la.la', result.task_id)
        self.assertEqual('la.la.la', result2.task_id)
