from __future__ import absolute_import

import warnings
from abc import abstractmethod, ABCMeta
from uuid import uuid5, NAMESPACE_URL

from dd.api.contexts.reader import DatasetReader
from dd.api.workflow.callables import LoadFileCallableBuilder
from dd.api.workflow.dataset import DatasetLoad, DatasetInit
from dd.api.workflow.readers import CSVReader

try:
    basestring  # Python 3
except NameError:
    basestring = str  # Python 3


class Context(object):
    """
    A context object will allow you to interact with your environnement, while
    building a DAG of tasks to be executed.
    """
    __metaclass__ = ABCMeta

    def __init__(self, workflow=None, db=None, uuid=None):
        self._workflow = workflow
        self._db = db
        self._task_id_counter = 0
        self.auto_persistence = False
        self.dependency_model = False
        self.default_write_options = {}
        if workflow is not None:
            self.uuid = uuid or str(uuid5(NAMESPACE_URL,
                                          self._workflow.dag_id)).replace('-', '')
        else:
            self.uuid = 55  # NOT IN MY HOUSE !!

    def increment_task_id_counter(self):
        """
        Increment by one the task id counter
        """
        self._task_id_counter += 1

    @property
    def task_id_counter(self):
        """
        Returns:
            current task id counter
        """
        return self._task_id_counter

    @property
    def db(self):
        """
        Returns:
            the database object associated to the context
        """
        return self._db

    @property
    def read(self):
        return DatasetReader(self)

    @property
    def workflow(self):
        """
        Returns:
            the workflow associated to the context
        """
        return self._workflow

    @abstractmethod
    def build_operator(self):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def build_dummy_operator(self):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def build_model_operator(self):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    def create_dataset(self, name):
        """
        Create a Dataset from a table

        This operation is *Lazy*

        Args:
            name: the new table name

        Returns:
            Dataset
        """
        return DatasetInit(self, name)

    # Deprecated
    def get_dataflow(self):
        """
        Deprecated. Use Context.workflow instead

        Returns:
            This context's dataflow
        """
        warnings.warn('Use property "workflow"', DeprecationWarning)
        return self.workflow

    # Deprecated
    def get_db(self):
        """
        Deprecated. Use Context.db instead

        Returns:
            This context's db object
        """
        warnings.warn('Use property "db"', DeprecationWarning)
        return self.db

    def load_file(self, filepath, table_name, write_options=None, reader=None,
                  normalize=True,
                  **read_options):
        """
        Reads a file and stores it into the database and return a
        Dataset object corresponding to this table.

        This operation is *Lazy*

        Args:
            filepath (str or file): the path to the csv to read
            table_name (str): the table name in the storage (in db)
            normalize (bool): True if you need to normalize the columns names
                before importing the data to the database. Normalizing means
                removing all characters that may interfere with Postgre's way
                of doing stuff (no UPPERCASE, special char, whitespace...)
            write_options (dict): a dictionary of options to be passed when
                writing the data (in db or in memory).
            reader: an object with a .read(path, **read_options) method which
                returns a pd.DataFrame.
            **read_options: keyword arguments to be used when reading the data
                from the csv file. All keywords from pandas.read_csv are 
                accepted.

        Returns:
            Dataset
        """
        assert isinstance(filepath, basestring), "Path must be a string"
        write_options = write_options or {}

        if reader is None:
            reader = CSVReader(**read_options)

        callable_builder = (LoadFileCallableBuilder(self)
                            .with_path(filepath)
                            .with_reader(reader)
                            .with_normalization(normalize)
                            .with_write_options(**write_options))
        return DatasetLoad(self, callable_builder, table_name)

    def load_model(self, address):
        """
        Retrieves a model from the database.

        Args:
            address: the address where the model is stored. The address must
                comply with the format name@schema.table

        Returns:
            A python object able to be fit or make predictions
        """
        name, table = address.split('@')
        model = self.db.retrieve_object(table, name)
        assert hasattr(model, 'fit')
        return self.model(model, address)

    @abstractmethod
    def model(self, model, model_address):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    def set_auto_persistence(self, boolean):
        """
        Sets the default option for persistence of Datasets and Models

        Args:
            boolean: the value of the option (True or False, False by default)
        """
        self.auto_persistence = boolean

    def set_default_write_options(self, **options):
        """
        Sets the default write options when writing Datasets to the database.

        Valid options may be found in the pandas.DataFrame.to_csv() method.

        Args:
            **options: options and values to be used as default options.
        """
        self.default_write_options = options

    def table(self, name):
        """
        Deprecated. Use Context.create_dataset instead.

        Creates a empty Dataset

        This operation is *Lazy*

        Args:
            name: the new table name

        Returns:
            Dataset
        """
        warnings.warn('Use the "create_dataset" method', DeprecationWarning)
        return self.create_dataset(name)

    @abstractmethod
    def _dataframe_from_table(self, table_name):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def _model_from_address(self, address):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def _save_object(self, object, address):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def _save_dataframe(self, dataframe, output_table):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def _save_model(self, model, table):
        """
        Abstract method. Raises NotImplementedError
        """
        raise NotImplementedError

    def _query(self, query):
        return self.db.query(query)

    def _execute(self, query):
        self.db.execute(query)
