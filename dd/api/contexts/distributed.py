from dd.api.contexts.base import Context
from dd.api.workflow.model import EmptyModel
from dd.operators import DDTransformation, DDModel
from airflow.operators.dummy_operator import DummyOperator

try:
    basestring  # Python 2
except NameError:
    basestring = str  # Python 3


class AirflowContext(Context):
    """
    The AirflowContext allows you to create an airflow DAG while describing the
    data transformations you want to implement.
    """

    def __init__(self, workflow, db, uuid=None):
        """
        Initialize a new Airflow context

        Args:
            workflow: a given workflow to execute
            db: the sink storage
            uuid: context identifier

        Returns:
            an Airflow Context
        """
        Context.__init__(self, workflow=workflow, db=db, uuid=uuid)
        self.name = "Airflow"
        self.auto_persistence = False
        self.dependency_model = True

    def build_dummy_operator(self, *args, **kwargs):
        """
        Returns an Airflow Dummy Operator instanciated with input arguments
        Args:
            *args: the arguments to be passed to the operator constructor.
            **kwargs: the keyword arguments to be passed to the operator
                constructor.
        Returns:
            A DummyOperator
        """
        return DummyOperator(dag=self.workflow, task_id=kwargs.get('task_id'))

    def build_operator(self, *args, **kwargs):
        """
        Returns an operator suited to the context (a DDTransformation).

        The DAG of the operator is set to the DAG of the context.

        Args:
            *args: the arguments to be passed to the operator constructor.
            **kwargs: the keyword arguments to be passed to the operator
                constructor.

        Returns:
            A DDTransformation
        """
        return DDTransformation(dag=self.workflow, *args, **kwargs)

    def build_model_operator(self, *args, **kwargs):
        """
        Returns an operator suited to the context (a DDModel).

        The DAG of the operator is set to the DAG of the context.

        Args:
            *args: the arguments to be passed to the operator constructor.
            **kwargs: the keyword arguments to be passed to the operator
                constructor.

        Returns:
            A DDModel
        """
        return DDModel(dag=self.workflow, *args, **kwargs)

    def model(self, model, model_address):
        """
        Returns an EmptyModel that can be fitted on a dataset
        
        Args:
            model: an object with a fit method

        Returns:
            an EmptyModel object
        """
        if isinstance(model, basestring):
            return self.load_model(model)
        assert hasattr(model, 'fit')
        return EmptyModel(self, model, model_address)

    def _dataframe_from_table(self, table_name):
        return self.db.retrieve_table(table_name)

    def _model_from_address(self, address):
        name, table = address.split('@')
        return self.db.retrieve_object(table, name)

    def _save_dataframe(self, dataframe, output_table, **import_options):
        """
        Save the dataset into the database.
        Args:
            dataset: the dataset to be saved
            **import_options: options to be passed to the database connection

        Returns:
            None
        """
        options = self.default_write_options.copy()
        options.update(import_options)
        self.db.import_dataframe(dataframe, output_table, **options)

    def _save_model(self, model, table):
        self._save_object(model, table)

    def _save_object(self, object, address):
        name, table = address.split('@')
        self.db.save_object(object, table, name)
