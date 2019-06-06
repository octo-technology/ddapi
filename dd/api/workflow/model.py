import logging

from dd import is_ipython
from dd.api.workflow.base import AbstractModel
from dd.api.workflow.callables import TransformCallableBuilder, \
    ModelFitCallableBuilder
from dd.api.workflow.dataset import DatasetTransformation
from dd.api.workflow.flow_functions import model_transformation, \
    model_prediction
from dd.api.workflow.utils import iterable
from dd.operators import NullOperator


class EmptyModel(AbstractModel):
    def __init__(self, context, model, model_address):
        self.context = context
        self.id = self.context.task_id_counter
        self.operator = None
        self.model = model
        self.name = model.__class__.__name__
        self.model_address = model_address
        self.executed = not is_ipython()
        self.is_cached = self.context.auto_persistence

    def fit(self, dataset, target=None, columns=None):
        """
        Returns an new model, waiting to be fit on the given dataset

        Args:
            dataset: The dataset on which the given model will be fit
            target: The column of the dataset containing the target
            columns: The columns to be used in the training set (target 
                excluded)
        
        Returns:
            a new model, soon to be fit
        """
        callable_builder = (ModelFitCallableBuilder(self.context)
                            .with_model(self)
                            .with_target(target)
                            .with_columns(columns))
        return Model(self.context, dataset, self.model,
                     self.model_address, callable_builder)

    def predict(self, model, target=None):
        """Will raise an error since the model is not fitted yet"""
        raise NotImplementedError("Can't predict yet, "
                                  "you must fit your model first")

    def transform(self, dataset, output_table=None, columns=None):
        """Will raise an error since the model is not fitted yet"""
        raise NotImplementedError("Can't transform yet, "
                                  "you must fit your model first")

    def collect(self):
        return self.model

    def __repr__(self):
        return "Model[{name}][{id}]".format(name=self.name,
                                            id=self.id)


class Model(AbstractModel):
    def __init__(self, context, dataset, model, model_address,
                 callable_builder):
        self.name = "Model"
        self.context = context
        self.id = self.context.task_id_counter
        self.predecessors = iterable(dataset)
        self.train_set = self.predecessors[0]

        self._callable = callable_builder.bind_to(self.train_set).build()
        self.operator = self._build_operator()
        self.executed = not is_ipython()

        self.model = model
        self.model_address = model_address
        self.output_table = dataset.output_table
        self.is_cached = self.context.auto_persistence

    def fit(self, dataset, target=None, columns=None):
        """
        Returns an new model, waiting to be fit on the given dataset
        :param dataset: The dataset on which the given model will be fit
        :param target: The column of the dataset containing the target
        :param columns: The columns on which the model will be applied
        :return: a new model, soon to be fit
        """
        callable_builder = (ModelFitCallableBuilder(self.context)
                            .with_model(self)
                            .with_target(target)
                            .with_columns(columns))
        return Model(self.context, dataset, self.model,
                     self.model_address, callable_builder)

    def predict(self, dataset, output_table=None, target=None, columns=None):
        """
        Returns a new dataset with the predictions made by the underlying model
        onto the dataset given as first parameter.
        :param dataset: the dataset containing the data on which the model has
         to make predictions
        :type output_table: string or None
        :param output_table: the name of the table in which the result of the
        join will be saved. If None is provided, the name is automatically
        generated and may be accessed via self.output_table
        :param target: if the provided dataset contains the target you wish to
        predict (because your dataset is a validation set), you may pass the
        target's column name so that it is dropped before making predictions.
        :param columns:  the columns on which the model will be applied
        :return: A new dataset with all the predictions.
        """
        callable_builder = (TransformCallableBuilder(self.context)
                            .with_transformation(model_prediction)
                            .with_kwargs(target=target, columns=columns))
        return DatasetTransformation([self, dataset], self.context,
                                     callable_builder, output_table)

    def transform(self, dataset, output_table=None, columns=None):
        """
        Returns a new dataset representing the result of the transformation
        learnt on the training data applied on the given dataset.
        
        Args:
            dataset (Dataset): the dataset to be transformed by the model.
            output_table (str): the name of the table in which the result of 
                the transformation will be saved. If None is provided, the name 
                is automatically generated and may be accessed via 
                self.output_table.
            columns (list of str): the columns on which the model will be 
                applied.

        Returns:
            A new dataset with the result of the predictions.
        """
        callable_builder = (TransformCallableBuilder(self.context)
                            .with_transformation(model_transformation)
                            .with_kwargs(columns=columns))
        return DatasetTransformation([self, dataset], self.context,
                                     callable_builder, output_table)

    def execute(self, force=False, recursive=False):
        """
        Executes the underlying operator so that it populates the table in the
        database. Calls the predecessors' execute method beforehand.
        """
        if not self.executed or force:
            self._execute_predecessors(force, recursive)
            model = self.operator.execute(context=None)
            self.executed = True
        else:
            model = self.context._model_from_address(self.model_address)

        if self.is_cached:
            self.model = model

        return model

    def _execute_predecessors(self, force, recursive):
        if recursive and not self.context.dependency_model:
            for predecessor in self.predecessors:
                predecessor.execute(recursive, force)
        else:
            logging.info("Context is {context}: operator executed in non-recursive mode.".format(context=self.context.name))


    def collect(self):
        """
        Returns the underlying model object.
        
        Returns:
            The underlying (fitted or not) model.
        """
        if self.executed and self.is_cached:
            return self.model
        else:
            model = self.execute()
            return model

    def save(self, address):
        self.execute()
        name, table = address.split("@")
        self.context.save_object(self.model, table, name)
        return self

    def _build_operator(self):
        """
        Builds an operator throught the context attribute.

        Returns:
            a DDOperator
        """
        operator = self.context.build_model_operator(
            python_callable=self._callable,
            provide_context=False,
            op_args=[], op_kwargs={},
            api_name=self.name,
            task_id=self.name + str(self.id))
        operator.set_upstream([p.operator for p in self.predecessors
                               if not isinstance(p.operator, NullOperator)])
        return operator

    def __repr__(self):
        return "Model[{name}][{id}]".format(name=self.name,
                                            id=self.id)

    def cache(self, boolean=True):
        self.is_cached = boolean
