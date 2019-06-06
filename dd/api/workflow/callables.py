from abc import abstractmethod
from functools import wraps

from dd.api.workflow.utils import normalize_columns


class CallableBuilder(object):
    # TODO : remove reference to dataset
    # replace with lighter reference to dataset.output_table
    def __init__(self, context, dataset=None):
        self.context = context
        self.dataset = dataset

    @abstractmethod
    def build(self):
        raise NotImplementedError("Callable Builder is an Abstract Class")


class TransformCallableBuilder(CallableBuilder):
    def __init__(self, context, dataset=None, transformation=None,
                 function_args=None, function_kwargs=None, write_options=None):
        super(TransformCallableBuilder, self).__init__(context, dataset)
        self.write_options = write_options or {}
        self.transformation = transformation
        self.function_kwargs = function_kwargs or {}
        self.function_args = function_args or []

    def bind_to(self, dataset):
        return TransformCallableBuilder(self.context,
                                        dataset,
                                        transformation=self.transformation,
                                        function_args=self.function_args,
                                        function_kwargs=self.function_kwargs,
                                        write_options=self.write_options)

    def build(self):
        @wraps(self.transformation)
        def run():
            function_args = [p.collect() for p in
                             self.dataset.predecessors
                             if hasattr(p, 'collect')]
            function_args += self.function_args
            new_df = self.transformation(*function_args,
                                         **self.function_kwargs)
            if type(new_df) is tuple:
                if type(self.dataset.output_table) is not tuple and len(self.dataset.output_table) != len(new_df):
                    raise ValueError("Invalid argument output_table: output table must be specified when function"
                                     " returns multiple dataframes")
                gen_write_options = []

                if type(self.write_options) is list and len(new_df) != len(self.write_options):
                    raise ValueError("Invalid argument write_options: if a list of write_options is specified, this"
                                     " list must be of the same size than the number of dataframes")
                elif type(self.write_options) is not list:
                    for i in range(len(self.dataset.output_table)):
                        gen_write_options.append(self.write_options)

                for i in range(len(self.dataset.output_table)):
                    df = new_df[i]
                    name = self.dataset.output_table[i]
                    opts = gen_write_options[i]
                    self.context._save_dataframe(df, name, **opts)
            else:
                self.context._save_dataframe(new_df,
                                         self.dataset.output_table,
                                         **self.write_options)

            return new_df

        return run

    def with_transformation(self, transformation):
        return TransformCallableBuilder(self.context,
                                        self.dataset,
                                        transformation=transformation,
                                        function_args=self.function_args,
                                        function_kwargs=self.function_kwargs,
                                        write_options=self.write_options)

    def with_kwargs(self, **kwargs):
        return TransformCallableBuilder(self.context,
                                        self.dataset,
                                        transformation=self.transformation,
                                        function_args=self.function_args,
                                        function_kwargs=kwargs,
                                        write_options=self.write_options)

    def with_write_options(self, **write_options):
        return TransformCallableBuilder(self.context,
                                        self.dataset,
                                        transformation=self.transformation,
                                        function_args=self.function_args,
                                        function_kwargs=self.function_kwargs,
                                        write_options=write_options)

    def with_args(self, *args):
        return TransformCallableBuilder(self.context,
                                        self.dataset,
                                        transformation=self.transformation,
                                        function_args=args,
                                        function_kwargs=self.function_kwargs,
                                        write_options=self.write_options)


class LoadFileCallableBuilder(CallableBuilder):
    def __init__(self, context, dataset=None, path=None,
                 reader=None, write_options=None, normalization=False):
        super(LoadFileCallableBuilder, self).__init__(context, dataset)

        self.path = path
        self.reader = reader
        self.write_options = write_options or {}
        self.normalization = normalization

    def bind_to(self, dataset):
        return LoadFileCallableBuilder(self.context,
                                       dataset,
                                       path=self.path,
                                       reader=self.reader,
                                       write_options=self.write_options,
                                       normalization=self.normalization)

    def build(self):
        def run():
            df = self.reader.read(self.path)
            if self.normalization:
                df.columns = normalize_columns(df.columns)
            self.context._save_dataframe(df,
                                         self.dataset.output_table,
                                         **self.write_options)
            return df

        return run

    def load(self):
        raise NotImplementedError

    def with_path(self, path):
        return LoadFileCallableBuilder(self.context,
                                       self.dataset,
                                       path=path,
                                       reader=self.reader,
                                       write_options=self.write_options,
                                       normalization=self.normalization)

    def with_reader(self, reader):
        return LoadFileCallableBuilder(self.context,
                                       self.dataset,
                                       path=self.path,
                                       reader=reader,
                                       write_options=self.write_options,
                                       normalization=self.normalization)

    def with_write_options(self, **write_options):
        return LoadFileCallableBuilder(self.context,
                                       self.dataset,
                                       path=self.path,
                                       reader=self.reader,
                                       write_options=write_options,
                                       normalization=self.normalization)

    def with_normalization(self, boolean):
        return LoadFileCallableBuilder(self.context,
                                       self.dataset,
                                       path=self.path,
                                       reader=self.reader,
                                       write_options=self.write_options,
                                       normalization=boolean)


class ModelFitCallableBuilder(CallableBuilder):
    def __init__(self, context, model=None, model_address=None,
                 dataset=None, target=None,
                 columns=None, write_options=None):
        super(ModelFitCallableBuilder, self).__init__(context, dataset)
        self.model = model
        self.model_address = model_address
        self.target = target
        self._columns = columns
        self.write_options = write_options or {}

    def bind_to(self, dataset):
        return ModelFitCallableBuilder(context=self.context,
                                       model=self.model,
                                       model_address=self.model_address,
                                       dataset=dataset,
                                       target=self.target,
                                       columns=self._columns,
                                       write_options=self.write_options)

    def build(self):
        def run():
            train_set = self.dataset.collect()
            model = self.model.collect()
            model_address = self.model.model_address
            columns = train_set.columns if self._columns is None \
                else self._columns

            if self.target is not None:
                X = train_set[columns].drop(self.target, axis=1)
                y = train_set[self.target]
                model.fit(X, y)
            else:
                X = train_set[columns]
                model.fit(X)

            self.context._save_model(model, model_address,
                                     **self.write_options)
            return model

        return run

    def with_columns(self, columns):
        return ModelFitCallableBuilder(context=self.context,
                                       model=self.model,
                                       model_address=self.model_address,
                                       dataset=self.dataset,
                                       target=self.target,
                                       columns=columns,
                                       write_options=self.write_options)

    def with_model(self, model):
        return ModelFitCallableBuilder(context=self.context,
                                       model=model,
                                       model_address=self.model_address,
                                       dataset=self.dataset,
                                       target=self.target,
                                       columns=self._columns,
                                       write_options=self.write_options)

    def with_target(self, target):
        return ModelFitCallableBuilder(context=self.context,
                                       model=self.model,
                                       model_address=self.model_address,
                                       dataset=self.dataset,
                                       target=target,
                                       columns=self._columns,
                                       write_options=self.write_options)

    def with_write_options(self, **write_options):
        return ModelFitCallableBuilder(context=self.context,
                                       model=self.model,
                                       model_address=self.model_address,
                                       dataset=self.dataset,
                                       target=self.target,
                                       columns=self._columns,
                                       write_options=write_options)


class QueryCallableBuilder(CallableBuilder):
    def __init__(self, context, query=None, dataset=None, create_table=False,
                 if_exists='replace'):
        super(QueryCallableBuilder, self).__init__(context)
        self.query = query
        self.dataset = dataset
        self.create_table = create_table
        self.if_exists = if_exists

    def bind_to(self, dataset):
        return QueryCallableBuilder(context=self.context,
                                    query=self.query,
                                    dataset=dataset,
                                    create_table=self.create_table,
                                    if_exists=self.if_exists)

    def build(self):
        def query():
            if not self.create_table:
                dataframe = self.context._query(self.query)
                self.context._save_dataframe(dataframe,
                                             self.dataset.output_table,
                                             index=False,
                                             if_exists=self.if_exists)
                return dataframe
            else:
                query = ""
                if self.if_exists == 'replace':
                    query = "DROP TABLE IF EXISTS {output_table};".format(
                        output_table=self.dataset.output_table)
                query += "CREATE TABLE {output_table} AS {query};".format(
                    output_table=self.dataset.output_table,
                    query=self.query)
                self.context._execute(query)

        return query

    def with_query(self, query):
        return QueryCallableBuilder(context=self.context,
                                    query=query,
                                    dataset=self.dataset,
                                    create_table=self.create_table,
                                    if_exists=self.if_exists)


class ActionCallableBuilder(CallableBuilder):
    def __init__(self, context, dataset=None, operation=None,
                 operation_args=None, operation_kwargs=None):
        super(ActionCallableBuilder, self).__init__(context, dataset)
        self.operation = operation
        self.operation_args = operation_args or []
        self.operation_kwargs = operation_kwargs or {}

    def bind_to(self, dataset):
        return ActionCallableBuilder(context=self.context,
                                     dataset=dataset,
                                     operation=self.operation,
                                     operation_args=self.operation_args,
                                     operation_kwargs=self.operation_kwargs)

    def build(self):
        @wraps(self.operation)
        def run():
            operation_args = [p.collect() for p in
                              self.dataset.predecessors
                              if hasattr(p, 'collect')]
            operation_args += self.operation_args

            self.operation(*operation_args, **self.operation_kwargs)

        return run

    def with_operation(self, operation):
        return ActionCallableBuilder(context=self.context,
                                     dataset=self.dataset,
                                     operation=operation,
                                     operation_args=self.operation_args,
                                     operation_kwargs=self.operation_kwargs)

    def with_kwargs(self, **kwargs):
        return ActionCallableBuilder(context=self.context,
                                     dataset=self.dataset,
                                     operation=self.operation,
                                     operation_args=self.operation_args,
                                     operation_kwargs=kwargs)

    def with_args(self, *args):
        return ActionCallableBuilder(context=self.context,
                                     dataset=self.dataset,
                                     operation=self.operation,
                                     operation_args=args,
                                     operation_kwargs=self.operation_kwargs)


class LoadDataFrameCallableBuilder(CallableBuilder):
    def __init__(self, context, dataset=None, dataframe=None, write_options=None):
        super(LoadDataFrameCallableBuilder, self).__init__(context, dataset)
        self.dataframe = dataframe
        self.write_options = write_options or {}

    def build(self):
        def run():
            self.context._save_dataframe(self.dataframe,
                                         self.dataset.output_table,
                                         **self.write_options)
            return self.dataframe
        return run

    def with_dataframe(self, dataframe):
        return LoadDataFrameCallableBuilder(context=self.context,
                                            dataset=self.dataset,
                                            dataframe=dataframe,
                                            write_options=self.write_options)

    def with_write_options(self, **write_options):
        return LoadDataFrameCallableBuilder(context=self.context,
                                            dataset=self.dataset,
                                            dataframe=self.dataframe,
                                            write_options=write_options)

    def bind_to(self, dataset):
        return LoadDataFrameCallableBuilder(context=self.context,
                                            dataset=dataset,
                                            dataframe=self.dataframe,
                                            write_options=self.write_options)


operators_map = {"transform": TransformCallableBuilder,
                 "load": LoadFileCallableBuilder,
                 "model_fit": ModelFitCallableBuilder,
                 "query": QueryCallableBuilder}
