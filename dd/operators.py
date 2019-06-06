import inspect
import logging
import os

from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults


class DDTransformation(PythonOperator):
    """
    Start a DD API function in a dataflow
    """

    ui_color = '#aff7a6'

    @apply_defaults
    def __init__(self, output_table, api_name=None, *args, **kwargs):
        super(DDTransformation, self).__init__(*args, **kwargs)
        self.output_table = output_table
        self.api_name = api_name
        self.result_dataframe = None

    @property
    def task_type(self):
        return "DDTransformation<%s>" % self.api_name

    def __repr__(self):
        return "<Task({name}>{api_name}): {task_id}>".format(
            name=self.__class__.__name__,
            api_name=self.api_name,
            task_id=self.task_id)

    def execute(self, **kwargs):
        """
        Execute the code specified by python_operator.
        Send a DataBase object in kwargs.

        Parameters
        ----------
        context operator context

        Returns
        -------
        the python_operator output
        """
        context = kwargs.pop('context', None)
        if self.provide_context:
            context.update(self.op_kwargs)
            context['templates_dict'] = self.templates_dict
            self.op_kwargs = context

        self.result_dataframe = self.python_callable(*self.op_args, **self.op_kwargs)
        return self.output_table

    def get_result(self):
        return self.result_dataframe


class DDModel(PythonOperator):
    """
    Start a DD API function in a dataflow
    """

    ui_color = '#af6e7a'

    @apply_defaults
    def __init__(self, api_name=None, *args, **kwargs):
        super(DDModel, self).__init__(*args, **kwargs)
        self.api_name = api_name

    @property
    def task_type(self):
        return "DDModel<%s>" % self.api_name

    def __repr__(self):
        return "<Task({name}>{api_name}): {task_id}>".format(
            name=self.__class__.__name__,
            api_name=self.api_name,
            task_id=self.task_id)

    def execute(self, **kwargs):
        """
        Execute the code specified by python_operator.
        Send a DataBase object in kwargs.

        Parameters
        ----------
        context operator context

        Returns
        -------
        the python_operator output
        """
        return super(DDModel, self).execute(kwargs)

    def __call__(self, **kwargs):
        """
        Execute the code specified by python_operator, sends a database object
        in parameter.
        """
        return self.execute(**kwargs)


class DDScript(PythonOperator):
    """
    Start a python function in a dataflow
    """

    ui_color = '#f4a460'

    def __init__(self, fun, output_table="", **kwargs):
        super(DDScript, self).__init__(python_callable=fun, **kwargs)
        self.output_table = output_table
        self.code = inspect.getmodule(fun).__file__

    def __call__(self, *args, **kwargs):
        return self.execute(None)


class DDNotebook(BaseOperator):
    """
    Start a python notebook in a dataflow
    """

    def __init__(self, file_name, output_table="", dargs={}):
        super(DDNotebook, self).__init__(**dargs)
        self.file_name = file_name
        self.output_table = output_table
        self.code = file_name

    def execute(self, context):
        cmd = "jupyter-nbconvert --ExecutePreprocessor.enabled=True %s --stdout > output/%s_%s.html" % (
            self.file_name,
            self.dag_id,
            self.task_id)
        logging.info(cmd)
        os.system(cmd)


class LocalOperator(object):
    def __init__(self, python_callable,
                 task_id,
                 output_table,
                 *args,
                 **kwargs):
        self.python_callable = python_callable
        self.output_table = output_table
        self.task_id = task_id
        self.result_dataframe = None

    def set_upstream(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        self.result_dataframe = self.python_callable(*args, **kwargs)
        return self.output_table

    def get_result(self):
        return self.result_dataframe


class LocalModel(object):
    def __init__(self, python_callable,
                 api_name,
                 task_id,
                 *args,
                 **kwargs):
        self.python_callable = python_callable
        self.api_name = api_name
        self.task_id = task_id

    def set_upstream(self, *args, **kwargs):
        pass

    def execute(self, *args, **kwargs):
        return self.python_callable()


class NullOperator(BaseOperator):
    def __init__(self):
        pass

    def execute(self, *args, **kwargs):
        pass

    def set_upstream(self, *args, **kwargs):
        pass

    def get_result(self):
        pass
