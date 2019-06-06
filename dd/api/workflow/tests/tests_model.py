import unittest

import numpy as np
import pandas as pd
from mock import MagicMock

from dd.api.contexts import LocalContext
from dd.api.workflow.dataset import Dataset
from dd.api.workflow.model import Model, EmptyModel


class TestModels(unittest.TestCase):
    def test_fit_should_return_new_model(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        model = EmptyModel(context, MagicMock(), "foo.bar")

        # When
        result = model.fit(dataset)

        # Check
        self.assertIsInstance(result, Model)

    def test_fit_on_a_fit_model_should_return_a_new_model(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        model = Model(context, MagicMock(), MagicMock(), "foo.bar", MagicMock())

        # When
        result = model.fit(dataset)

        # Check
        self.assertIsInstance(result, Model)

    def test_execute_should_call_underlying_operator_execute(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        scikit_model = MagicMock()
        model = EmptyModel(context, scikit_model, "foo.bar")
        fitted_model = model.fit(dataset)
        fitted_model.executed = False

        # When
        _ = fitted_model.execute()

        # Check
        fitted_model.operator.execute.assert_called_once()

    def test_predict_method_returns_a_dataset(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        scikit_model = MagicMock()
        model = EmptyModel(context, scikit_model, "foo.bar")

        # When
        predictions = (model
                       .fit(dataset, target="foo")
                       .predict(dataset, target="foo"))

        # Check
        self.assertIsInstance(predictions, Dataset)

    def test_predict_call_underlying_model_predict(self):
        # Given
        dataset = MagicMock()
        scikit_model = MagicMock()
        scikit_model.estimators = [MagicMock()]
        array = np.array([0, 1, 2, 3])
        scikit_model.predict.return_value = array

        db = MagicMock()
        db.retrieve_object.return_value = (scikit_model, {})
        context = LocalContext(db=db)

        model = (EmptyModel(context, scikit_model, "model@foo.bar")
                 .fit(dataset, target="foo"))
        predictions = (model
                       .predict(dataset, target="foo"))

        # When
        df = predictions.collect()

        # Check
        scikit_model.predict.assert_called_once()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (len(array), 1))
        self.assertEqual(df.columns.tolist(), ["foo_pred"])

    def test_transform_method_returns_a_dataset(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        scikit_model = MagicMock()
        model = EmptyModel(context, scikit_model, "foo.bar")

        # When
        result = (model
                  .fit(dataset)
                  .transform(dataset))

        # Check
        self.assertIsInstance(result, Dataset)

    def test_collect_should_return_underlying_model(self):
        # Given
        dataset = MagicMock()
        context = MagicMock()
        scikit_model = MagicMock()
        scikit_model.estimators_ = [MagicMock()]  # Emulates fitted model
        model = EmptyModel(context, scikit_model, "foo.bar").fit(dataset)

        # When
        result = model.collect()

        # Check
        self.assertIs(result, scikit_model)

    def test_empty_model_collect_should_return_underlying_model(self):
        # Given
        context = MagicMock()
        scikit_model = MagicMock()
        model = EmptyModel(context, scikit_model, "foo.bar")

        # When
        result = model.collect()

        # Check
        self.assertIs(result, scikit_model)
