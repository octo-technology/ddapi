import logging
import sys

from numpy.random.mtrand import RandomState

from dd.dataflow import is_ipython
from dd.operators import NullOperator
from airflow.operators.dummy_operator import DummyOperator
from .actions import Action
from .base import AbstractDataset
from .callables import TransformCallableBuilder, ActionCallableBuilder
from .flow_functions import join, split_train_or_test, select_columns
from .sql import SQLOperator
from .utils import iterable, get_output_table, get_schema


class Dataset(AbstractDataset):
    """
    This class represents your data and provides an interface to transform it
    or apply operations to it. You probably do not want to instanciate this
    class yourself; you should use a Context to create Datasets.
    """

    def __init__(self, context, output_table):
        self.context = context
        self.output_table = output_table
        self.id = self.context.task_id_counter
        self.context.increment_task_id_counter()
        self.dataframe = None
        self.operator = NullOperator()
        self.predecessors = []
        self.executed = not is_ipython()
        self.name = "Dataset"
        self.is_cached = self.context.auto_persistence
        self.sql_operator = SQLOperator(self.context, self)

    def __getitem__(self, columns):
        return self.select_columns(columns)

    def __repr__(self):
        return ("{context_name}{dataset_name}[{table}][{id}]"
                .format(context_name=self.context.name,
                        dataset_name=self.name,
                        table=self.output_table,
                        id=self.id))

    def get_output_table(self):
        return self.output_table

    @property
    def columns(self):
        """
        Returns the column names of the underlying dataframe

        Returns:
            a pandas.Index
        """
        return self.collect().columns

    @property
    def memory_usage(self):
        """
        Returns the size, in bytes, of the memory occupied by this object.

        Note that it is only a lower bound on the actual size, since some
        objects may not return their full size.

        Returns (int):
            The estimated size in bytes of the memory taken by this object.
        """
        dataframe = self.collect()
        try:
            dataframe_size = dataframe.memory_usage().sum()
        except AttributeError:
            dataframe_size = 0
        return dataframe_size + sys.getsizeof(self)

    @property
    def shape(self):
        """
        Returns the shape of the dataframe

        Returns:
            a Tuple of int
        """
        return self.collect().shape

    @property
    def sql(self):
        return self.sql_operator

    def apply(self, operation, *operation_args, **operation_kwargs):
        callable_builder = (ActionCallableBuilder(self.context)
                            .with_operation(operation)
                            .with_kwargs(**operation_kwargs)
                            .with_args(*operation_args))
        return Action(self.context, callable_builder, self)

    def cache(self, boolean=True):
        self.is_cached = boolean

    def collect(self):
        """
        Returns a pandas.DataFrame representing the data of the dataset.

        Fetches data from the database in the table self.output_table.
        Connection to the database is made thanks to the context.

        Returns:
            a pandas.DataFrame
        """
        if self.dataframe is not None:
            return self.dataframe
        else:
            dataframe = self.execute()
            return dataframe

    def execute(self, force=False, recursive=False):
        """
        Executes the underlying operator and returns its return value.

        The operations defined on datasets are lazy and this method must be
        called so that the operation is ran. By default, if the dataset has
        already been executed, the underlying operator won't be ran again.
        You may execute recursively all the predecessors of this dataset by
        setting recursive to True

        Args:
            force (bool): default False, set it to True to force the execution
                of the operator when already executed.
            recursive (bool): default False, set it to True to execute all the
                predecessors recursively.

        Returns:
            A dataframex
        """
        if not self.executed or force:
            self._execute_predecessors(force, recursive)
            self.operator.execute()
            dataframe = self.operator.get_result()
            self.executed = True
        else:
            if type(self.output_table) is list:
                dataframe = []
                for otable in self.output_table:
                    dataframe.append(self.context._dataframe_from_table(otable))
            else:
                dataframe = self.context._dataframe_from_table(self.output_table)

        if self.is_cached:
            self.dataframe = dataframe

        return dataframe

    def head(self, n=5):
        """
        Returns the n first row of the dataset.

        Args:
            n (int): default 5, the number of rows to be returned

        Returns:
            a pandas.DataFrame with n rows
        """
        if is_ipython():
            return self.collect().head(n)
        else:
            logging.info("When dataset.head() is called not in IPython Notebook, it is ignored")

    def join(self, other, output_table=None,
             write_options=None, **join_kwargs):
        """
        Joins two datasets.

        Args:
            other (Dataset): an AirflowDataset, considered to be the right part
                of the join
            output_table (string or None): the name of the table in which the
                result of the join will be saved. If None is provided, the name
                is automatically generated and may be accessed via
                self.output_table
            write_options (dictionary or None): dictionary of arguments to be
                passed to the context when saving the results to the database.
            **join_kwargs: see pandas.merge() options

        Returns:
            a new AirflowTransformation
        """
        write_options = write_options or {}
        join_callable = (TransformCallableBuilder(self.context)
                         .with_transformation(join)
                         .with_kwargs(**join_kwargs)
                         .with_write_options(**write_options))
        return DatasetTransformation([self, other], self.context,
                                     join_callable, output_table)

    def select_columns(self, columns, output_table=None, write_options=None):
        """
        Allows to select a subset of columns of the given dataset.

        Args:
            columns (list of str): the list of columns to be selected
            output_table (str): the table in which the result will be saved
            write_options (dict or None): dictionary of arguments to be
                passed to the context when saving the results to the database.

        Returns:

        """
        write_options = write_options or {}
        callable_builder = (TransformCallableBuilder(self.context)
                            .with_write_options(**write_options)
                            .with_transformation(select_columns(columns)))
        return DatasetTransformation(self, self.context,
                                     callable_builder, output_table)

    def split_train_test(self, train_size=None, seed=None, write_options=None):
        """
        Randomly split the given Dataset in two distinct Datasets.

        Args:
            train_size (float, int or None): If float, should be between 0.0
                and 1.0 and represent the proportion of the dataset to include
                in the train split. If int, represents the absolute number of
                train samples. If None, default is 0.75.
            seed (int or None): seed from pseudo-random state generator
            write_options (dictionary or None): dictionary of arguments to be
                passed to the context when saving the results to the database.
        Returns:
            TWO new AirflowTransformations
        """
        state = RandomState(seed)
        write_options = write_options or {}

        train_and_test = []
        for name in ("train", "test"):
            transformation = split_train_or_test(name, train_size, state)
            callable_builder = (TransformCallableBuilder(self.context)
                                .with_transformation(transformation)
                                .with_write_options(**write_options))
            dataset = DatasetTransformation(self,
                                            self.context,
                                            callable_builder,
                                            self.get_output_table() + "_" + name)
            train_and_test.append(dataset)

        return tuple(train_and_test)

    def to_operator(self):
        """
        Returns the PythonOperator of the current dataset.

        Provided for compatibility with the current API.

        Returns:
            a dd.DDOperator
        """
        return self.operator

    def multitransform(self, function, *function_args, **function_kwargs):
        """
        Returns multiple datasets created from applying the given function to the
        current dataset

        Args:
            function (callable): a function taking a pandas.DataFrame as its
                first parameter and returning a pandas.DataFrame
            output_tables (list): the name of the tables in which the result of
                the transformation will be saved.
            datasets (datasets iterable): set of datasets to be passed to the
                transform function
            write_options (dictionary or None): dictionary of arguments to be
                passed to the context when saving the results to the database.
            **function_kwargs: keyword argument to be passed to the function
                passed as a parameter

        Returns:
            a new AirflowTransformation
        """
        output_tables = function_kwargs.pop("output_tables", None)
        datasets = function_kwargs.pop("datasets", None)
        write_options = function_kwargs.pop("write_options", None)
        datasets = [self] + list(datasets or [])
        write_options = write_options or {}
        callable_builder = (TransformCallableBuilder(self.context)
                            .with_transformation(function)
                            .with_args(*function_args)
                            .with_kwargs(**function_kwargs)
                            .with_write_options(**write_options))

        dtransfo = DatasetTransformation(datasets, self.context,
                                         callable_builder, output_tables)

        if type(output_tables) is list and len(output_tables) > 1:
            result = []
            for id in range(len(output_tables)):
                result.append(MultiDatasetTransformation(dtransfo, id))
            return tuple(result)
        else:
            raise ValueError("Invalid output_tables arguments: a list must be specified")

    def transform(self, function, *function_args, **function_kwargs):
        """
        Returns a new dataset created from applying the given function to the
        current dataset

        Args:
            function (callable): a function taking a pandas.DataFrame as its
                first parameter and returning a pandas.DataFrame
            output_table (str): the name of the table in which the result of
                the transformation will be saved. If None is provided, the name
                is automatically generated and may be accessed via
                self.output_table
            datasets (datasets iterable): set of datasets to be passed to the
                transform function
            write_options (dictionary or None): dictionary of arguments to be
                passed to the context when saving the results to the database.
            **function_kwargs: keyword argument to be passed to the function
                passed as a parameter

        Returns:
            a new AirflowTransformation
        """
        output_table = function_kwargs.pop("output_table", None)
        datasets = function_kwargs.pop("datasets", None)
        write_options = function_kwargs.pop("write_options", None)
        datasets = [self] + list(datasets or [])
        write_options = write_options or {}
        callable_builder = (TransformCallableBuilder(self.context)
                            .with_transformation(function)
                            .with_args(*function_args)
                            .with_kwargs(**function_kwargs)
                            .with_write_options(**write_options))

        return DatasetTransformation(datasets, self.context,
                                     callable_builder, output_table)

    def _execute_predecessors(self, force, recursive):
        if recursive:
            for predecessor in self.predecessors:
                predecessor.execute(recursive, force)


class DatasetInit(Dataset):
    def __init__(self, context, output_table):
        super(DatasetInit, self).__init__(context, output_table)
        self.name = "Init"
        self.operator = self._build_operator()

    def execute(self, recursive=False, force=False):
        return self.context._dataframe_from_table(self.output_table)

    def _build_operator(self):
        id = self.context.task_id_counter
        task_id = self.name + str(id)
        operator = self.context.build_dummy_operator(python_callable=None,
                                                     provide_context=False,
                                                     op_args=[], op_kwargs={},
                                                     api_name="load_file",
                                                     output_table=self.output_table,
                                                     task_id=task_id)
        operator.set_upstream([p.operator for p in self.predecessors
                               if not isinstance(p.operator, NullOperator)])
        return operator


class DatasetLoad(Dataset):
    def __init__(self, context, callable_builder, output_table):
        """
        This is the abstraction used to load data from a file into a Dataset.

        The file represented by the filepath must be a csv file.

        Args:
            context (Context object): the context from which the original
                dataset was created.
            output_table (str): the name of the output table where the result
                will be stored.
        """
        super(DatasetLoad, self).__init__(context, output_table)
        self._callable = callable_builder.bind_to(self).build()
        self.operator = self._build_operator()
        self.name = "Load"

    def _build_operator(self):
        """
        Builds an operator throught the context attribute.

        Returns:
            Operator
        """
        task_id = self.output_table.replace('/', '.')
        operator = self.context.build_operator(python_callable=self._callable,
                                               provide_context=False,
                                               op_args=[], op_kwargs={},
                                               api_name="load_file",
                                               output_table=self.output_table,
                                               task_id=task_id)
        operator.set_upstream([p.operator for p in self.predecessors
                               if not isinstance(p.operator, NullOperator)])
        return operator


class DatasetTransformation(Dataset):
    def __init__(self, dataset, context, callable_builder, output_table=None):
        """
        A datasetTransformation is an abstraction representing the result of an
        operation onto a dataset.

        This operation is given a 'function', which takes one or several
        dataframe as input(s) and returns a new dataset.

        Args:
            dataset (Dataset or list(Dataset)): the parent dataset(s)
            context (Context object): the context from which the original
                dataset was created
            function (callable): the function that implements the
                transformation that will be applied to the input dataset(s)
            output_table (str): the name of the output table where the result
                will be stored
        """
        super(DatasetTransformation, self).__init__(context, output_table)
        self.predecessors = iterable(dataset)
        self._callable = callable_builder.bind_to(self).build()

        self.output_table = get_output_table(output_table, self)
        self.operator = self._build_operator()
        self.name = "Transformation"

    def _build_operator(self):
        """
        Builds an operator throught the context attribute.

        Returns:
            a DDOperator
        """
        if type(self.output_table) is list:
            output_table = '-'.join(self.output_table)
        else:
            output_table = self.output_table
        task_id = output_table.replace('/', '.')
        operator = self.context.build_operator(python_callable=self._callable,
                                               provide_context=False,
                                               op_args=[], op_kwargs={},
                                               api_name="transformation",
                                               output_table=output_table,
                                               task_id=task_id)
        operator.set_upstream([p.operator for p in self.predecessors
                               if not isinstance(p.operator, NullOperator)])
        return operator


class MultiDatasetTransformation(Dataset):
    def __init__(self, transformation, transformation_id):
        '''
        A MultiDatasetTransformation is an abstraction representing the result of an
        operation that generates multiple datasets.

        This operation is given a 'function', which takes one or several
        dataframe as input(s) and returns several datasets.
        
        :param transformation: the shared DatasetTransformation object 
        :param transformation_id: the id of the Dataset
        '''
        super(MultiDatasetTransformation, self).__init__(transformation.context, transformation.output_table)
        self.transformation = transformation
        self.transformation_id = transformation_id

    def _build_operator(self):
        '''
        Returns the build operator of the shared transformation object
        '''
        return self.transformation._build_operator()

    def execute(self, force=False, recursive=False):
        '''
        Returns the result of the current dataset (selected by id)
        '''
        result = self.transformation.execute(force=force, recursive=recursive)
        return result[self.transformation_id]

    def collect(self):
        '''
        Route the collect operation to the specified Dataset
        '''
        result = self.transformation.collect()
        return result[self.transformation_id]

    def get_output_table(self):
        '''
        Returns the current output table
        '''
        return self.output_table[self.transformation_id]
