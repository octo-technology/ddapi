import unittest

import pandas as pd
import mock
from datetime import datetime
from dd.api.contexts.distributed import AirflowContext
from dd.api.workflow.dataset import Dataset
from dd.api.workflow.model import EmptyModel
from airflow import DAG


class TestAirflowContext(unittest.TestCase):

    def setUp(self):
        self.workflow = mock.MagicMock(spec=DAG("mock", start_date=datetime.now()))
        self.workflow.dag_id = "mock"
        self.db = mock.MagicMock()
        self.context = AirflowContext(self.workflow, self.db)

    def test_table_method_should_return_Dataset_instance(self):
        # Given
        context = self.context

        # When
        view = context.table("mytable")

        # Assert
        self.assertIsInstance(view, Dataset)

    def test_load_file_should_add_task_to_workflow(self):
        # Given
        context = self.context

        # When
        dataset = context.load_file("stub.csv", "table")

        # Assert
        self.workflow.add_task.assert_called_once()

    def test_build_dummy_operator_should_raise_when_kwargs_contain_no_task_id(self):
        # Given
        context = self.context
        kwargs = {}

        # Assert
        with self.assertRaises(TypeError) as te:
            # When
            context.build_dummy_operator(**kwargs)
        self.assertEquals(str(te.exception), 'The key has to be a string')

    def test_create_dataset_should_call_dummy_operator_builder(self):
        # Given
        context = self.context

        # Assert
        with mock.patch('dd.api.contexts.distributed.AirflowContext.build_dummy_operator') as p:
            # When
            dataset = context.create_dataset("some_table_name")
            p.assert_called_once()

    def test_create_dataset_should_add_task_to_workflow(self):
        # Given
        context = self.context

        # When
        dataset = context.create_dataset("some_table_name")

        # Assert
        self.workflow.add_task.assert_called_once()

    def test_context_should_raise_attribute_error_if_model_doesnt_have_fit_method(
            self):
        # Given
        context = self.context
        model = mock.MagicMock(spec_set=object)

        # Assert
        with self.assertRaises(AssertionError):
            # When
            context.model(model, "foo.model")

    def test_context_should_return_empty_model_if_model_has_fit_method(self):
        # Given
        context = self.context
        model = mock.MagicMock()
        model.fit.return_value = None

        # When
        dd_model = context.model(model, "foo.model")

        # Assert
        self.assertIsInstance(dd_model, EmptyModel)

    def test_context_should_call_db_to_save_dataframe(self):
        # Given
        context = self.context
        dataframe = mock.MagicMock()
        output_table = "output_table"

        # When
        context._save_dataframe(dataframe, output_table, if_exists='replace')

        # Check
        self.db.import_dataframe.assert_called_once_with(dataframe,
                                                         "output_table",
                                                         if_exists='replace')

    def test_dataframe_from_table_should_call_db_retrieve_table(self):
        # Given
        context = self.context

        # When
        df = context._dataframe_from_table("table")

        # Check
        self.db.retrieve_table.assert_called_once_with("table")

    def test_model_should_return_empty_model(self):
        # Given
        context = self.context
        scikit_model = mock.MagicMock()

        # When
        result = context.model(scikit_model, "model@foo.bar")

        # Check
        self.assertIsInstance(result, EmptyModel)

    def test_load_model_should_return_model(self):
        # Given
        context = self.context

        # When
        result = context.load_model("model@foo.bar")

        # Check
        self.assertIsInstance(result, EmptyModel)

    def test_save_object_should_call_db_save_object(self):
        # Given
        context = self.context
        object_to_save = mock.MagicMock()

        # When
        context._save_object(object_to_save, "object@foo.bar")

        # Check
        self.db.save_object.assert_called_once_with(object_to_save,
                                                    "foo.bar",
                                                    "object")

    def test_save_model_should_call_db_save_object(self):
        # Given
        context = self.context
        model = mock.MagicMock()

        # When
        context._save_model(model, "object@foo.bar")

        # Check
        self.db.save_object.assert_called_once_with(model,
                                                    "foo.bar",
                                                    "object")

    def test_model_from_address_should_call_db_retrieve_object(self):
        # Given
        context = self.context

        # When
        context._model_from_address("object@foo.bar")

        # Check
        self.db.retrieve_object.assert_called_once_with("foo.bar",
                                                        "object")

    def test_set_auto_persistence_should_set_auto_persistence_attribute(self):
        # Given
        context = self.context

        # When
        context.set_auto_persistence(True)

        # Check
        self.assertTrue(context.auto_persistence)

        # Then when
        context.set_auto_persistence(False)

        # Check
        self.assertFalse(context.auto_persistence)

    def test_set_default_write_options_should_set_new_default_values(self):
        # Given
        context = self.context

        # When
        context.set_default_write_options(if_exists="replace")

        # Check
        self.assertEqual(context.default_write_options,
                         {'if_exists': 'replace'})

        # Then when
        context.set_default_write_options(if_exists="append", index=False)

        # Check
        self.assertEqual(context.default_write_options,
                         {'if_exists': 'append', 'index': False})

    def test__query_should_return_dataframe(self):
        # Given
        context = self.context
        self.db.query.return_value = pd.DataFrame()

        # When
        result = context._query("select * from foo.bar")

        # Check
        self.assertIsInstance(result, pd.DataFrame)

    def test__query_should_call_db_query(self):
        # Given
        context = self.context

        # When
        result = context._query("select * from foo.bar")

        # Check
        self.db.query.assert_called_once_with("select * from foo.bar")
