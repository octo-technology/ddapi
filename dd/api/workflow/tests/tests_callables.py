import unittest

from mock import MagicMock

from dd.api.workflow.callables import TransformCallableBuilder, \
    LoadFileCallableBuilder, ModelFitCallableBuilder, QueryCallableBuilder, \
    ActionCallableBuilder, LoadDataFrameCallableBuilder
from dd.api.workflow.dataset import Dataset
from dd.api.workflow.readers import CSVReader
from airflow import DAG


class AbstractCallablebuilderTest(unittest.TestCase):
    def setUp(self):
        self.dataset = MagicMock()
        self.dataset.output_table = "output_table"
        self.context = MagicMock()
        self.context.workflow = MagicMock(spec=DAG)
        self.context.workflow.params = {}
        self.context.workflow.task_dict = {}


class TestTransformCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestTransformCallableBuilder, self).setUp()
        self.callable_builder = TransformCallableBuilder(self.context,
                                                         self.dataset)

    def test_bind_to_should_return_new_builder_with_given_dataset(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        builder = TransformCallableBuilder(context)

        # When
        new_builder = builder.bind_to(dataset)

        # Check
        self.assertIsInstance(new_builder, TransformCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.dataset, dataset)

    def test_bind_to_is_commutative(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        transformation = MagicMock()
        builder = TransformCallableBuilder(context)

        # When
        new_builder0 = (builder.bind_to(dataset)
                        .with_transformation(transformation))
        new_builder1 = (builder
                        .with_transformation(transformation)
                        .bind_to(dataset))

        # Check
        self.assertIs(new_builder0.dataset, new_builder1.dataset)
        self.assertIs(new_builder0.transformation, new_builder1.transformation)

    def test_operator_with_transformation_returns_new_operator_with_given_callable(
            self):
        # Given
        callable_builder = self.callable_builder
        transformation = MagicMock()

        # When
        new_builder = callable_builder.with_transformation(transformation)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, TransformCallableBuilder)
        self.assertIs(new_builder.transformation, transformation)

    def test_operator_with_kwargs_returns_new_operator_with_given_kwargs(self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_kwargs(string="string", bool=True)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, TransformCallableBuilder)
        self.assertEqual(new_builder.function_kwargs, dict(string="string",
                                                           bool=True))

    def test_operator_with_write_options_returns_new_operator_with_given_options(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_write_options(string="string",
                                                          bool=True)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, TransformCallableBuilder)
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_operator_methods_may_be_chained(self):
        # Given
        callable_builder = self.callable_builder
        transformation = MagicMock()

        # When
        new_builder = (callable_builder
                       .with_transformation(transformation)
                       .with_kwargs(string="string", bool=True)
                       .with_write_options(string="string", bool=True))

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, TransformCallableBuilder)
        self.assertEqual(new_builder.transformation, transformation)
        self.assertEqual(new_builder.function_kwargs, dict(string="string",
                                                           bool=True))
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_operator_execute_should_call_context_save_dataframe(self):
        # Given
        callable_builder = self.callable_builder
        transformation = MagicMock()
        transformation.__name__ = "mock"
        operator = callable_builder.with_transformation(transformation).build()

        # When
        operator()

        # Check
        self.context._save_dataframe.assert_called_once()


class TestLoadCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestLoadCallableBuilder, self).setUp()
        self.callable_builder = LoadFileCallableBuilder(self.context,
                                                        self.dataset)

    def test_operator_with_path_should_return_new_operator_with_given_path(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_path("/path/to/file")

        # Check
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder.path, "/path/to/file")

    def test_operator_with_read_options_should_return_new_operator_with_given_options(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_reader(CSVReader(sep=",",
                                                             header=None))

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertEqual(new_builder.reader.read_options,
                         dict(sep=",", header=None))

    def test_operator_with_write_options_returns_new_operator_with_given_options(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_write_options(string="string",
                                                          bool=True)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_operator_with_normalization_returns_new_operator_with_given_bool(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_normalization(True)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertEqual(new_builder.normalization, True)

    def test_operator_methods_may_be_chained(self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = (callable_builder
                       .with_path("/path/to/file")
                       .with_normalization(True)
                       .with_reader(CSVReader(sep=",", header=None))
                       .with_write_options(string="string",
                                           bool=True))

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertEqual(new_builder.path, "/path/to/file")
        self.assertEqual(new_builder.reader.read_options,
                         dict(sep=",", header=None))
        self.assertEqual(new_builder.normalization, True)
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_bind_to_should_return_new_builder_with_given_dataset(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        builder = LoadFileCallableBuilder(context)

        # When
        new_builder = builder.bind_to(dataset)

        # Check
        self.assertIsInstance(new_builder, LoadFileCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.dataset, dataset)

    def test_bind_to_is_commutative(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        path = "/path/to/file.foo"
        builder = LoadFileCallableBuilder(context)

        # When
        new_builder0 = (builder.bind_to(dataset)
                        .with_path(path))
        new_builder1 = (builder
                        .with_path(path)
                        .bind_to(dataset))

        # Check
        self.assertIs(new_builder0.dataset, new_builder1.dataset)
        self.assertIs(new_builder0.path, new_builder1.path)


class TestModelFitCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestModelFitCallableBuilder, self).setUp()
        self.callable_builder = ModelFitCallableBuilder(self.context,
                                                        self.dataset)

    def test_operator_with_model_should_return_new_operator_with_given_model(
            self):
        # Given
        callable_builder = self.callable_builder
        model = MagicMock()

        # When
        new_builder = callable_builder.with_model(model)

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder.model, model)

    def test_operator_with_target_should_return_new_operator_with_given_target(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_target("foo")

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder.target, "foo")

    def test_operator_with_columns_should_return_new_operator_with_given_columns(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_columns(["foo", "bar", "baz"])

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder._columns, ["foo", "bar", "baz"])

    def test_operator_with_write_options_returns_new_operator_with_given_options(
            self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_write_options(string="string",
                                                          bool=True)

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_bind_to_should_return_new_builder_with_given_dataset(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        builder = ModelFitCallableBuilder(context)

        # When
        new_builder = builder.bind_to(dataset)

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.dataset, dataset)

    def test_operator_methods_may_be_chained(self):
        # Given
        callable_builder = self.callable_builder
        model = MagicMock()

        # When
        new_builder = (callable_builder
                       .with_model(model)
                       .with_target("foo")
                       .with_write_options(string="string",
                                           bool=True))

        # Check
        self.assertIsInstance(new_builder, ModelFitCallableBuilder)
        self.assertIsNot(new_builder, callable_builder)
        self.assertEqual(new_builder.model, model)
        self.assertEqual(new_builder.target, "foo")
        self.assertEqual(new_builder.write_options, dict(string="string",
                                                         bool=True))

    def test_function_returned_by_build_call_context_save_model_method(self):
        # Given
        callable_builder = self.callable_builder
        model = MagicMock()
        wrapped_model = MagicMock()
        model.collect.return_value = wrapped_model
        model_address = "model@schema.table"
        model.model_address = model_address
        new_builder = (callable_builder
                       .with_model(model)
                       .with_target("foo")
                       .with_write_options(string="string",
                                           bool=True)
                       .bind_to(self.dataset))
        callable_function = new_builder.build()

        # When
        _ = callable_function()

        # Check
        self.context._save_model.assert_called_once_with(wrapped_model,
                                                         model_address,
                                                         string="string",
                                                         bool=True)

    def test_function_returned_by_build_call_model_fit_method(self):
        # Given
        callable_builder = self.callable_builder
        model = MagicMock()
        wrapped_model = MagicMock()
        model.collect.return_value = wrapped_model
        model_address = "model@schema.table"
        model.model_address = model_address
        new_builder = (callable_builder
                       .with_model(model)
                       .with_target("foo")
                       .with_write_options(string="string",
                                           bool=True)
                       .bind_to(self.dataset))
        callable_function = new_builder.build()

        # When
        _ = callable_function()

        # Check
        wrapped_model.fit.assert_called_once()

    def test_bind_to_is_commutative(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        path = "/path/to/file.foo"
        builder = LoadFileCallableBuilder(context)

        # When
        new_builder0 = (builder.bind_to(dataset)
                        .with_path(path))
        new_builder1 = (builder
                        .with_path(path)
                        .bind_to(dataset))

        # Check
        self.assertIs(new_builder0.dataset, new_builder1.dataset)
        self.assertIs(new_builder0.path, new_builder1.path)


class TestQueryCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestQueryCallableBuilder, self).setUp()
        self.callable_builder = QueryCallableBuilder(self.context)

    def test_with_query_should_return_new_builder_with_given_query(self):
        # Given
        builder = self.callable_builder
        query = "select * from foo.bar"

        # When
        new_builder = builder.with_query(query)

        # Check
        self.assertIsInstance(new_builder, QueryCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.query, query)

    def test_bind_to_should_return_new_builder_with_given_dataset(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        builder = QueryCallableBuilder(context)

        # When
        new_builder = builder.bind_to(dataset)

        # Check
        self.assertIsInstance(new_builder, QueryCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.dataset, dataset)

    def test_operator_execute_should_call_context_save_dataframe(self):
        # Given
        dataset = MagicMock()
        callable_builder = self.callable_builder
        query = "select * from foo.bar"
        operator = callable_builder.with_query(query).bind_to(dataset).build()

        # When
        operator()

        # Check
        self.context._query.assert_called_once_with(query)


class TestActionCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestActionCallableBuilder, self).setUp()
        self.callable_builder = ActionCallableBuilder(self.context)

    def test_bind_to_should_return_new_builder_with_given_dataset(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        builder = ActionCallableBuilder(context)

        # When
        new_builder = builder.bind_to(dataset)

        # Check
        self.assertIsInstance(new_builder, ActionCallableBuilder)
        self.assertIsNot(new_builder, builder)
        self.assertEqual(new_builder.dataset, dataset)

    def test_bind_to_is_commutative(self):
        # Given
        context = MagicMock()
        dataset = MagicMock()
        operation = MagicMock()
        builder = ActionCallableBuilder(context)

        # When
        new_builder0 = (builder.bind_to(dataset)
                        .with_operation(operation))
        new_builder1 = (builder
                        .with_operation(operation)
                        .bind_to(dataset))

        # Check
        self.assertIs(new_builder0.dataset, new_builder1.dataset)
        self.assertIs(new_builder0.operation, new_builder1.operation)

    def test_operator_with_transformation_returns_new_operator_with_given_callable(
            self):
        # Given
        callable_builder = self.callable_builder
        operation = MagicMock()

        # When
        new_builder = callable_builder.with_operation(operation)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, ActionCallableBuilder)
        self.assertIs(new_builder.operation, operation)

    def test_operator_with_kwargs_returns_new_operator_with_given_kwargs(self):
        # Given
        callable_builder = self.callable_builder

        # When
        new_builder = callable_builder.with_kwargs(string="string", bool=True)

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, ActionCallableBuilder)
        self.assertEqual(new_builder.operation_kwargs, dict(string="string",
                                                            bool=True))

    def test_operator_methods_may_be_chained(self):
        # Given
        callable_builder = self.callable_builder
        operation = MagicMock()

        # When
        new_builder = (callable_builder
                       .with_operation(operation)
                       .with_kwargs(string="string", bool=True))

        # Check
        self.assertIsNot(new_builder, callable_builder)
        self.assertIsInstance(new_builder, ActionCallableBuilder)
        self.assertEqual(new_builder.operation, operation)
        self.assertEqual(new_builder.operation_kwargs, dict(string="string",
                                                            bool=True))

    def test_operator_execute_should_call_operation(self):
        # Given
        callable_builder = self.callable_builder
        predecessor = MagicMock(spec_set=Dataset)
        dataframe = MagicMock()
        predecessor.collect.return_value = dataframe
        self.dataset.predecessors = [predecessor]
        operation = MagicMock()
        operation.__name__ = "mock"
        operator = (callable_builder
                    .with_operation(operation)
                    .with_kwargs(foo="bar", baz=1)
                    .bind_to(self.dataset).build())

        # When
        operator()

        # Check
        operation.assert_called_once_with(dataframe, foo="bar", baz=1)


class TestLoadDataFrameCallableBuilder(AbstractCallablebuilderTest):
    def setUp(self):
        super(TestLoadDataFrameCallableBuilder, self).setUp()
        self.callable_builder = LoadDataFrameCallableBuilder(self.context)

    def test_build_should_return_function(self):
        # Given
        callable_builder = self.callable_builder

        # When
        result = callable_builder.build()

        # Check
        self.assertTrue(callable(result))

    def test_build_should_call_context_save_dataframe_method(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(2, 2),
                                 columns=list("ab"))
        callable_builder = (self.callable_builder
                            .with_dataframe(dataframe)
                            .bind_to(self.dataset))
        callable_function = callable_builder.build()

        # When
        _ = callable_function()

        # Check
        self.context._save_dataframe.assert_called_once_with(dataframe,
                                                             self.dataset.output_table)

    def test_build_should_call_context_save_dataframe_method_with_given_write_options(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(2, 2),
                                 columns=list("ab"))
        callable_builder = (self.callable_builder
                            .with_dataframe(dataframe)
                            .with_write_options(if_exists='append')
                            .bind_to(self.dataset))
        callable_function = callable_builder.build()

        # When
        _ = callable_function()

        # Check
        self.context._save_dataframe.assert_called_once_with(dataframe,
                                                             self.dataset.output_table,
                                                             if_exists='append')

    def test_built_function_should_return_dataframe(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(2, 2),
                                 columns=list("ab"))
        callable_builder = (self.callable_builder
                            .with_dataframe(dataframe)
                            .bind_to(self.dataset))
        callable_function = callable_builder.build()

        # When
        result = callable_function()

        # Check
        self.assertIs(result, dataframe)

    def test_method_ordering_does_not_matter(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(2, 2),
                                 columns=list("ab"))
        write_options = dict(foo="bar", baz="yo")

        # When
        callable_builder1 = (self.callable_builder
                             .with_dataframe(dataframe)
                             .with_write_options(**write_options)
                             .bind_to(self.dataset))
        callable_builder2 = (self.callable_builder
                             .with_write_options(**write_options)
                             .with_dataframe(dataframe)
                             .bind_to(self.dataset))

        # Check
        for builder in (callable_builder1, callable_builder2):
            self.assertIs(builder.context, self.context)
            self.assertIs(builder.dataset, self.dataset)
            self.assertIs(builder.dataframe, dataframe)
            self.assertEqual(builder.write_options, write_options)
