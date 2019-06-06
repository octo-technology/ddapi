import unittest

import pandas as pd
import mock

from dd.api.contexts.local import LocalContext
from dd.api.contexts.reader import DatasetReader
from dd.api.workflow.dataset import DatasetInit
from dd.api.workflow.model import EmptyModel


class TestLocalContext(unittest.TestCase):
    def test_local_context_load_from_table_should_return_DbInit_instance(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        dataset = context.table("mytable")

        # Assert
        self.assertIsInstance(dataset, DatasetInit)

    def test_local_context_read_should_return_reader(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        reader = context.read

        # Assert
        self.assertIsInstance(reader, DatasetReader)

    def test_create_dataset_should_call_dummy_operator_builder(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # Assert
        with mock.patch('dd.api.contexts.local.LocalContext.build_dummy_operator') as p:
            # When
            dataset = context.create_dataset("some_table_name")
            p.assert_called_once()

    def test_save_dataset_should_call_db_import_dataframe(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)
        dataframe = mock.MagicMock()
        output_table = "foo.bar"

        # When
        context._save_dataframe(dataframe, output_table)

        # Check
        db.import_dataframe.assert_called_once_with(dataframe,
                                                    "foo.bar")

    def test_dataframe_from_table_should_call_db_retrieve_table(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        context._dataframe_from_table("foo.bar")

        # Check
        db.retrieve_table.assert_called_once_with("foo.bar")

    def test_dataframe_from_table_should_pass_limit(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        context._dataframe_from_table("foo.bar")

        # Check
        db.retrieve_table.assert_called_once_with("foo.bar")

    def test_set_auto_persistence_to_False_changes_default_value(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)
        self.assertTrue(context.auto_persistence)

        # When
        context.set_auto_persistence(False)

        # Check
        self.assertFalse(context.auto_persistence)

    def test_set_auto_persistence_to_False_forces_dataset_to_stop_caching(
            self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)
        context.set_auto_persistence(False)
        dataset = context.create_dataset("foo")
        new_dataset = dataset.transform(lambda x: x)

        # When
        new_dataset.collect()

        # Check
        self.assertIs(new_dataset.dataframe, None)

    def test_model_should_return_empty_model(self):
        # Given
        db = mock.MagicMock()
        scikit_model = mock.MagicMock()
        context = LocalContext(db)

        # When
        result = context.model(scikit_model, "model@foo.bar")

        # Check
        self.assertIsInstance(result, EmptyModel)

    def test_load_model_should_return_model(self):
        # Given
        db = mock.MagicMock()
        scikit_model = mock.MagicMock()
        db.retrieve_object.return_value = scikit_model
        context = LocalContext(db)

        # When
        result = context.load_model("model@foo.bar")

        # Check
        self.assertIsInstance(result, EmptyModel)

    def test_save_object_should_call_db_save_object(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)
        object_to_save = mock.MagicMock()

        # When
        context._save_object(object_to_save, "object@foo.bar")

        # Check
        db.save_object.assert_called_once_with(object_to_save,
                                               "foo.bar",
                                               "object")

    def test_model_from_address_call_db_retrieve_object(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        model = context._model_from_address("model@foo.bar")

        # Check
        self.assertIsNotNone(model)
        db.retrieve_object.assert_called_once_with("foo.bar", "model")

    def test_save_model_should_call_db_save_object(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)
        model = mock.MagicMock()

        # When
        context._save_model(model, "object@foo.bar")

        # Check
        db.save_object.assert_called_once_with(model,
                                               "foo.bar",
                                               "object")

    def test_set_default_write_options_should_set_new_default_values(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

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
        db = mock.MagicMock()
        db.query.return_value = pd.DataFrame()
        context = LocalContext(db)

        # When
        result = context._query("SELECT * FROM foo.bar")

        # Check
        self.assertIsInstance(result, pd.DataFrame)

    def test__query_should_call_db_query(self):
        # Given
        db = mock.MagicMock()
        context = LocalContext(db)

        # When
        result = context._query("SELECT * FROM foo.bar")

        # Check
        db.query.assert_called_once_with("SELECT * FROM foo.bar")
