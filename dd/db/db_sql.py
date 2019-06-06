from __future__ import absolute_import

import json
import logging
import re
import string
import warnings

import six
import pickle
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column, Integer, desc, PickleType, Text, select, VARCHAR

VALID_COLUMN_CHARACTERS = set(string.ascii_letters + string.digits + "_-")

REDSHIFT = 'redshift'
SQLITE = 'sqlite'
POSTGRES = 'postgres'


def check_and_format_sqlname(name):
    '''
    Check if the name is compatible with SQL column/table names
    :param name: the table or column name
    :return: Boolean
    '''
    if not isinstance(name, six.string_types):
        return False
    if re.match(r"^[_a-z]+(_|\d|[a-z])*(\.([a-z]+(_|\d|[a-z])*()))?$", name):
        return True
    else:
        return False


def normalize_column_name(col):
    '''
    This function normalize column names (cast to string, ignoring non-ascii, lowering column names)
    '''
    if not isinstance(col, six.string_types):
        col = str(col)
    new_name = str([x for x in col if x in VALID_COLUMN_CHARACTERS]).lower()
    normalized_name = ""
    for char in new_name:
        if re.match(r"([a-z0-9]|_|\.)", str(char)):
            normalized_name += char
    if re.match(r"[0-9]", normalized_name[0]):
        normalized_name = "c" + normalized_name
    return normalized_name


class DB(object):

    # TODO remove the following function when remove db.py dependency
    def _create_legacy_DB(self):
        import db as dbpy
        from db.db import TableSet
        from db.queries import mssql as mssql_templates
        from db.queries import mysql as mysql_templates
        from db.queries import postgres as postgres_templates
        from db.queries import sqlite as sqlite_templates

        params = {attr: getattr(dbpy.DB, attr) for attr in dir(dbpy.DB)}
        for attr in dir(DB):
            params[attr] = getattr(DB, attr)
        params['__init__'] = lambda x, *args, **kwargs: None
        del params['__delete__']

        tmp_type = type('DB_legacy', (dbpy.DB, DB), params)
        legacy = object.__new__(tmp_type)
        for attr in self.__dict__:
            setattr(legacy, attr, getattr(self, attr))
        legacy._tables = TableSet([])
        query_templates = {
            "mysql": mysql_templates,
            "postgres": postgres_templates,
            "redshift": postgres_templates,
            "sqlite": sqlite_templates,
            "mssql": mssql_templates,
        }
        legacy._query_templates = query_templates[self.dbtype].queries
        if legacy.dbtype == SQLITE:
            legacy._create_sqlite_metatable()
        return legacy

    # TODO remove the following function when remove db.py dependency
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        if hasattr(DB, attr):
            return object.__getattribute__(self, attr)

        import db
        if hasattr(db.DB, attr):
            warnings.warn(DeprecationWarning('Method "{}" is deprecated and will be removed soon'.format(attr)))
            db = self._create_legacy_DB()
            return getattr(db, attr)
        else:
            raise AttributeError("{} instance has no attribute '{}'".format(type(self), attr))

    def __init__(self, username=None, password=None, hostname='localhost',
                 port=None, filename=None, dbname=None, dbtype=None,
                 schemas=None,
                 profile=None, exclude_system_tables=True, limit=1000,
                 keys_per_column=None, driver=None, cache=False, echo_mode=False):

        self.__engine = None
        self.__connection = None
        if profile:  # If profile is specified, load config from airflow connection info
            self.profile = profile
            self._load_info_from_profile()

        else:
            self.dbname = dbname
            self.dbtype = dbtype
            self.hostname = hostname
            self.password = password
            self.port = port
            self.username = username
            self.filename = filename

        self._exclude_system_tables = exclude_system_tables
        self._use_cache = cache
        self.schemas = schemas
        self.limit = limit
        self.keys_per_column = keys_per_column
        self.driver = driver
        self.echo_mode = echo_mode

        if self.dbtype is None and self.profile is None:
            raise Exception(
                "'dtype' not specified! Must select one of: postgres, sqlite, mysql, mssql, or redshift; \n" +
                "or you can provide a 'profile' from airflow connections")

    def _load_info_from_profile(self):
        """
            Loads connection parameters from a Airflow profile.
            When profile is None, do nothing
        """
        assert (self.profile is not None)
        from dd.ddhook import DDHook

        hook = DDHook(self.profile)
        conn = hook.get_connection(self.profile)
        logging.info('Loading configuration from profile {}'.format(self.profile))
        self.dbname = conn.schema
        self.dbtype = conn.conn_type
        self.hostname = conn.host
        self.password = conn.password
        self.port = conn.port
        self.username = conn.login
        self.filename = conn.schema

    @property
    def con(self):
        """
            Returns the engine connection. It creates the engine if needed.
        """
        if not self.__engine:
            self.create_engine()
        return self.__connection

    @property
    def engine(self):
        """
            Returns the engine. It creates the engine if needed.
        """
        if not self.__engine:
            self.create_engine()
        return self.__engine

    @property
    def cur(self):
        """
            Returns cursor from connection. It creates the engine if needed.
        """
        if not self.__engine:
            self.create_engine()
        return self.__cursor

    @property
    def uri(self):
        """
            Returns URI of the connection. It creates the engine if needed.
            Required an active connection.
        """
        if not self.__engine:
            self.create_engine()
        return self.__uri

    def create_engine(self):
        """
            Create the sqlalchemy engine. It creates the uri, conn and cur
        """

        if self.dbtype == SQLITE:
            self.__uri = self._get_connection_str_sqlite(self.filename)
        else:
            self.__uri = self._get_connection_str(self.dbname, self.dbtype, self.hostname, self.password, self.port,
                                                  self.username)
        if self.dbtype == POSTGRES:
            engine = create_engine(self.__uri, echo=self.echo_mode, isolation_level="AUTOCOMMIT")
        else:
            engine = create_engine(self.__uri, echo=self.echo_mode)

        self.__engine = engine
        self.__connection = engine.connect().connection
        self.__cursor = self.__connection.cursor()

    def __delete__(self):
        if self.__engine:
            self.__engine.dispose()

    def _get_connection_str_sqlite(self, filename):
        return "sqlite:///{filename}".format(filename=filename)

    def _get_connection_str(self, dbname, dbtype, hostname, password, port, username):
        if dbtype == POSTGRES or dbtype == REDSHIFT:
            pg_suffix = 'ql'
            dbtype = dbtype + pg_suffix
        return "{dbtype}://{user}:{password}@{host}:{port}/{dbname}".format(
            dbtype=dbtype,
            host=hostname,
            user=username,
            password=password,
            dbname=dbname,
            port=port)

    def import_dataframe(self, df, name, fast_mode=False, normalize_columns=False, **kwargs):
        """
        Serializes and stores a pandas.DataFrame in the database.
        If postgreSQL dbtype is detected, we create the schema before insert.
        By default, import_dataframe uses pandas.DataFrame.to_sql() function,
        when fast_mode is activated, uses Blaze's Odo project very fast importers


        Parameters
        ----------
        :param df: The pandas.DataFrame you want to store.
        :param name: The table name where it will be saved. Support {schema}.{table} pattern for postgreSQL
        :param fast_mode: False by default, uses blaze's Odo bulk insert on every Odo supported Databases
        :param normalize_columns: False by default, automatically rename columns to match sql column names
        :type df: pandas.DataFrame
        :type name: str
        :type fast_mode: bool
        :return: The full table name, with the schema if it is provided with the name argument

        Raises
        ------
        ValueError when timezone-aware data is found in the dataframe.
        c.f. FAQ in documentation

        Examples
        --------
        >>> from dd import DB
        >>> import pandas
        >>> df = pandas.DataFrame(np.random.randn(10, 5))
        >>> db = DB(dbtype='sqlite', filename=':memory:')
        >>> db.import_dataframe(df, 'titanic.train')
        >>> db.import_dataframe(df, 'titanic.train', fast_mode=True) # using bulk insert feature of the underlying database
        """

        table_with_schema = name

        if 'index' in kwargs:
            warnings.warn(DeprecationWarning('"index" parameter is not supported anymore'))
        kwargs['index'] = False

        if normalize_columns:
            df.columns = map(normalize_column_name, df.columns)

        # checking if column names have a good SQL format
        invalid_col_names = [cname for cname in df.columns if not check_and_format_sqlname(cname)]

        if invalid_col_names:
            raise ValueError('Invalid column names ' + str(
                invalid_col_names) + '. Column names must be compatible with SQL column types.')

        schema, table = self._split_schema_and_table(table_with_schema)
        self._ensure_schema(schema)

        if fast_mode:
            from odo import discover
            from odo.backends import sql_csv, sql
            uri = '{uri}::{table}'.format(uri=self.uri, table=table)
            dshape = discover(df)
            self._create_or_replace_table(df, kwargs, schema, table)
            table = sql.resource_sql(self.uri, table, bind=self.engine, schema=schema, dshape=dshape, **kwargs)
            result = sql_csv.append_dataframe_to_sql_table(table, df, bind=self.engine, dshape=dshape, **kwargs)
            if result is None:
                raise Exception("Import DataFrame failed in fast_mode")
        else:
            with self.engine.begin() as connection:
                df.to_sql(name=table, con=connection, schema=schema, **kwargs)

        return table_with_schema

    def _create_or_replace_table(self, df, kwargs, schema, table):
        df.head(0).to_sql(name=table, con=self.engine, schema=schema, **kwargs)

    def _ensure_schema(self, schema):
        if schema is not None and (self.dbtype == POSTGRES or self.dbtype == REDSHIFT):
            self.engine.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(schema))

    def _split_schema_and_table(self, name):
        if self.dbtype == POSTGRES:
            splits = str.split(name, '.')
            if len(splits) >= 2:
                schema, table_name = splits[0], splits[1]
                return schema, table_name
        return None, name

    def save_object(self, obj, model_table, name, metadata={}, drop_if_exists=False):
        """
        Serializes and stores an object in the database.
        Args:
            obj: The object you want to store.
            model_table: The model table, where all models are (or will be) saved.
            name: The name the object will be given inside the database.
            metadata: metadata data linked to model
            drop_if_exists: If True, the table used to store the objects will be dropped first. BE CAREFUL. No return
            from drop_if_exists=True.

        Returns:
            The id of the model. The id is unique and may be stored for later use as a unique identifier of the object.
        """

        with self.engine.begin():
            schema, table_name = self._split_schema_and_table(model_table)
            table = self._create_model_table(table_name, schema)
            if drop_if_exists:
                d_stmt = table.delete().where(table.c.name == name)
                self.engine.execute(d_stmt)
            stmt = table.insert().values([{"name": name,
                                           "pickle": obj,
                                           "metadata": json.dumps(metadata)}])
            self.engine.execute(stmt)

        result = self.engine.execute(select([table.c.id]).order_by(desc(table.c.id)))
        model_id = result.fetchone()['id']
        return model_id

    def _create_model_table(self, table_name, schema=None):
        self._ensure_schema(schema)

        models, meta = self._get_models_table_sqla_object(table_name, schema)

        meta.create_all(self.engine)
        return models

    def retrieve_table(self, table_name):
        """
        Returns:
            :return: a table as a pandas.DataFrame
        Args:
            :param table_name: The name of the table to be retrieved
        """
        from pandas import read_sql_table
        schema, table = self._split_schema_and_table(table_name)
        return read_sql_table(table_name=table, con=self.engine, schema=schema)

    def retrieve_object(self, model_table, name, model_id=None):
        """
        Returns the latest model (greatest id) whose name equals name or model_id=id (model_id has priority)
        Args:
            name: the name of the model
            model_id: the id of the model. If given, search is performed over id, not over name.

        Returns:
            the unpickled object stored in the database.
            the metadata stored in the database. The unique id is stored in the field "id" in the metadata dictionary.
        """

        schema, table = self._split_schema_and_table(model_table)
        models, _ = self._get_models_table_sqla_object(table, schema)

        query = select([models.c.pickle, models.c.metadata, models.c.id]).select_from(models)
        if model_id is not None:
            stmt = query.where(models.c.id == model_id)
        else:
            stmt = query.where(models.c.name == name)

        result = self.engine.execute(stmt.compile(self.engine))
        row_1 = result.fetchone()
        model, metadata, obj_id = row_1.pickle, row_1.metadata, row_1.id
        metadata_json = json.loads(metadata)
        metadata_json["id"] = obj_id
        return model, metadata_json

    def execute(self, *args, **kwargs):
        return self.engine.execute(*args, **kwargs)

    def read_sql(self, query):
        from pandas.io.sql import read_sql
        return read_sql(query, self.engine)

    def from_dataframe(self, df, name, **kwargs):
        return self.import_dataframe(df, name, **kwargs)

    def get_column_names(self, table_name):
        """
        Gets the names of the columns of a table
        :param table_name: Name of the table
        :return: A list of column names
        """
        metadata = MetaData(self.engine, reflect=True)
        table = metadata.tables[table_name]
        return table.columns.keys()

    def drop_table(self, table_name):
        '''
        Drops a table. If the table doesn't exist, no error is raised
        :param table_name: Name of the table
        '''
        schema, table = self._split_schema_and_table(table_name)
        metadata = MetaData(self.engine, reflect=True, schema=schema)
        if table_name in metadata.tables:
            table = metadata.tables[table_name]
            table.drop(self.engine)

    def drop_view(self, view_name):
        '''
        Drops a view. If the view doesn't exist, no error is raised
        :param view_name: Name of the view
        '''
        self.drop_table(view_name)

    def _get_models_table_sqla_object(self, table_name, schema):
        metadata = MetaData(schema=schema)
        models = Table(table_name, metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       # name =
                       # VARCHAR instead of TEXT for MySQL because
                       # https://stackoverflow.com/questions/1827063/mysql-error-key-specification-without-a-key-length
                       Column('name', VARCHAR(255), nullable=False, unique=True),
                       Column('pickle', PickleType(protocol=pickle.HIGHEST_PROTOCOL, pickler=pickle), nullable=False),
                       Column('metadata', Text, nullable=True)
                       )
        return models, metadata
