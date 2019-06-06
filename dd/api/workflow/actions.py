import logging

from dd.dataflow import is_ipython
from dd.api.workflow.utils import get_output_table
from dd.operators import NullOperator


class Action(object):
    def __init__(self, context, callable_builder, dataset):
        self.context = context
        self.id = self.context.task_id_counter
        self.context.increment_task_id_counter()
        self._callable = callable_builder.bind_to(self).build()
        self.predecessors = [dataset]
        self.name = "Action"
        self.task_id = get_output_table(None, self)
        self.executed = not is_ipython()
        self.operator = self._build_operator()

    def __call__(self):
        """
        Equivalent to self.execute(force=False, recursive=False)
        """
        self.execute(force=False, recursive=False)

    def __repr__(self):
        return ("{context_name}{dataset_name}[{task_id}][{id}]"
                .format(context_name=self.context.name,
                        dataset_name=self.name,
                        task_id=self.task_id,
                        id=self.id))

    def execute(self, force=False, recursive=False):
        """
        Runs the underlying operation.

        Args:
            force: force the execution even if the Action has already been
                executed
            recursive: tries to execute the predecessors of the Action. The
                *force* parameter if passed recursively to the predecessors.

        Returns:
            None
        """
        if not self.executed or force:
            self._execute_predecessors(force, recursive)
            self.operator.execute()
            self.executed = True

    def _build_operator(self):
        """
        Builds an operator throught the context attribute.

        Returns:
            a DDOperator
        """
        operator = self.context.build_operator(python_callable=self._callable,
                                               provide_context=False,
                                               op_args=[], op_kwargs={},
                                               api_name="transformation",
                                               output_table=self.predecessors[0].output_table,
                                               task_id=self.task_id)
        operator.set_upstream([p.operator for p in self.predecessors
                               if not isinstance(p.operator, NullOperator)])
        return operator

    def _execute_predecessors(self, force, recursive):
        if recursive and not self.context.dependency_model:
            for predecessor in self.predecessors:
                predecessor.execute(recursive, force)
        else:
            logging.info("Context is {context}: operator executed in non-recursive mode.".format(context=self.context.name))

 
