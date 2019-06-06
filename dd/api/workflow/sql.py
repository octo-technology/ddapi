from dd.api.workflow.actions import Action
from .callables import QueryCallableBuilder, ActionCallableBuilder


class SQLOperator(object):
    def __init__(self, context, dataset):
        self.context = context
        self.parent_dataset = dataset

    def query(self, query, output_table=None, create_table=False, if_exists='replace'):
        """
        Returns a new Dataset representing the result of a query.

        Args:
            query: the query to be executed in the database
            output_table: the name of the table where the result will be
                stored. If not provided, a name is generated for you.
            create_table: if True, a new table is created directly in SQL, the pandas is lazy
            if_exists: if table exists then the default is to 'replace'

        Returns:
            A new Dataset
        """
        from .dataset import DatasetTransformation
        callable_builder = (QueryCallableBuilder(self.context, create_table=create_table, if_exists=if_exists)
                            .with_query(query))
        return DatasetTransformation(self.parent_dataset, self.context,
                                     callable_builder, output_table)

    def execute(self, query):
        def execute_with_context(dataset, query):
            # Ignores first argument passed from the parent dataset
            # (see closure 'run' in .callables.ActionCallableBuilder.build())
            self.context._execute(query)

        callable_builder = (ActionCallableBuilder(self.context)
                            .with_operation(execute_with_context)
                            .with_kwargs(query=query))
        return Action(self.context, callable_builder, self.parent_dataset)
