# -*- coding: utf-8 -*-
import tempfile
import unittest
from datetime import timedelta

import numpy as np
import pandas as pd
import six
from datashape import dshape
from mock import MagicMock, patch, PropertyMock
from pandas.util.testing import assert_contains_all
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import MetaData, Table, Column, Text

from dd.db.db_sql import DB, check_and_format_sqlname, normalize_column_name


class TestDBAdapter(unittest.TestCase):

    def test__init__nominal(self):
        db = self._get_testdb()

        self.assertEqual(db.uri, 'sqlite:///:memory:')

    def test_get_connection_str_sqlite(self):
        # given
        db = self._get_testdb()
        filename = "/tmp/yolo/db.sqlite"

        # when
        result = db._get_connection_str_sqlite(filename)

        # then
        self.assertEqual(result, 'sqlite:////tmp/yolo/db.sqlite')

    def test_import_dataframe_should_store_df_as_table_and_return_the_name(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[['a', 'b', 'c'], ['a', 'b', 'c']], columns=['c1', 'c2', 'c3'])
        table_name = 't_test'

        # when
        result = db.import_dataframe(df, table_name)

        # then
        self.assertEqual(result, 't_test')

    def test_import_dataframe_should_store_df_as_table_and_return_the_fullname_with_schema(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[['a', 'b', 'c'], ['a', 'b', 'c']], columns=['c1', 'c2', 'c3'])
        table_name = 'titanic.t_test'

        # when
        result = db.import_dataframe(df, table_name)

        # then
        self.assertEqual(result, 'titanic.t_test')

    def test_import_dataframe_should_override_table_if_exists(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[['a', 'b', 'c'], ['a', 'b', 'c']], columns=['c1', 'c2', 'c3'])
        table_name = 't_test'

        conn = db.engine.connect()
        conn.execute("DROP TABLE IF EXISTS t_test")
        conn.execute("CREATE TABLE t_test (c1 TEXT, c2 TEXT, c3 TEXT)")
        conn.execute("INSERT INTO t_test VALUES ('a', 'b', 'c')")

        # when
        _ = db.import_dataframe(df, table_name, if_exists='replace')

    def test_import_dataframe_should_drop_non_ascii_character_on_columns(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[], columns=['c1ĉ', 'c2€', 'c3àéö'])
        table_name = 't_test'

        # when
        result = db.import_dataframe(df, table_name, index=True, normalize_columns=True)

        # then
        metadata = MetaData(db.engine, reflect=True)
        table = metadata.tables[result]
        self.assertListEqual(['c1', 'c2', 'c3'], table.columns._data._list)

    def test_import_dataframe_should_lower_case_column_name(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[], columns=['C1', 'C2', 'C3'])
        table_name = 't_test'

        # when
        result = db.import_dataframe(df, table_name, index=True, normalize_columns=True)

        # then
        metadata = MetaData(db.engine, reflect=True)
        table = metadata.tables[result]
        self.assertListEqual(['c1', 'c2', 'c3'], table.columns._data._list)

    def test_import_dataframe_when_fast_mode_is_true_should_use_odo_bulk_import_underthehood(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[['a', 'b', 'c'], ['a', 'b', 'c']], columns=['c1', 'c2', 'c3'])
        table_name = 't_test'
        odo_mock = MagicMock(return_value=df)

        # when
        with patch('odo.backends.sql_csv.append_dataframe_to_sql_table', odo_mock):
            result = db.import_dataframe(df, table_name, fast_mode=True, index=False)

        # then
        self.assertEqual(result, 't_test')
        odo_mock.assert_called_once_with(
            TableMatcher(Table('t_test', MetaData(bind=db.engine),
                               Column('c1', Text()),
                               Column('c2', Text()),
                               Column('c3', Text()),
                               schema=None)),
            df,
            bind=db.engine,
            dshape=dshape("2 * {c1: ?string, c2: ?string, c3: ?string}"),
            index=False)
        # pd.testing.assert_frame_equal(result)

    def test_split_schema_and_table_should_return_schema_and_table_name(self):
        # given
        db = self._get_testdb()
        table_name = 'schema.t_test'
        db.dbtype = 'postgres'

        # when
        r_schema, r_table = db._split_schema_and_table(table_name)

        # then
        self.assertEqual(r_table, 't_test')
        self.assertEqual(r_schema, 'schema')

    def test_split_schema_and_table_should_return_the_fulltablename_if_dbtype_not_postgres(self):
        # given
        db = self._get_testdb()
        table_name = 'schema.t_test'
        db.dbtype = 'mysql'

        # when
        r_schema, r_table = db._split_schema_and_table(table_name)

        # then
        self.assertEqual(r_table, 'schema.t_test')
        self.assertEqual(r_schema, None)

    def test_get_connection_str(self):
        # given
        db = self._get_testdb()
        dbname, dbtype, hostname, password, port, username = 'datadriver_db', 'mysql', 'localhost', 'root', 3306, 'root'
        dbtype_pg = 'postgres'

        # when
        result = db._get_connection_str(dbname, dbtype, hostname, password, port, username)
        result_pg = db._get_connection_str(dbname, dbtype_pg, hostname, password, port, username)

        # then
        self.assertEqual(result, 'mysql://root:root@localhost:3306/datadriver_db')
        self.assertEqual(result_pg, 'postgresql://root:root@localhost:3306/datadriver_db')

    def test_get_connection_str_should_add_QL_when_dbtype_is_postgres_or_redshift(self):
        # given
        db = self._get_testdb()
        dbname, dbtype, hostname, password, port, username = 'datadriver_db', 'postgres', 'localhost', 'root', 5432, 'root'

        # when
        result_pg = db._get_connection_str(dbname, dbtype, hostname, password, port, username)

        # then
        self.assertEqual(result_pg, 'postgresql://root:root@localhost:5432/datadriver_db')

    def test_retrieve_table_should_select_table_return_table_in_dataframe(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(data=[[1, 2], [11, 22]], columns=['c1', 'c2'])
        classic_table = 't_test'
        df.to_sql(name=classic_table, con=db.engine)
        postgres_table = 'titanic.t_test'
        df.to_sql(name=postgres_table, con=db.engine)

        # when
        result = db.retrieve_table(table_name=classic_table)
        _ = db.retrieve_table(table_name=postgres_table)

        # then
        expected = pd.DataFrame(data=[[0, 1, 2], [1, 11, 22]], columns=['index', 'c1', 'c2'])
        pd.testing.assert_frame_equal(result, expected)

    def test_save_and_retrieve_object_should_return_the_model_and_metadata_from_table(self):
        # given
        db = self._get_testdb()
        model = RandomForestClassifier(max_depth=3, random_state=0)
        X = pd.DataFrame(data=[[1, 2], [3, 4], [11, 3]], columns=['a', 'b'])
        test = pd.DataFrame(data=[[2, 2], [1, 1]], columns=['a', 'b'])
        y = [3, 7, 14]
        model.fit(X, y)
        metadata = {'yolo_field': 'yolo !'}

        # when
        saved_id = db.save_object(model, 't_test_models', 'my_model', metadata=metadata)
        result, result_meta = db.retrieve_object(model_table='t_test_models', name='my_model')

        # then
        six.assertCountEqual(self, result.__dict__, model.__dict__)
        pd.np.testing.assert_array_equal(result.predict(test), model.predict(test))
        self.assertEqual(result_meta, {'yolo_field': 'yolo !', 'id': saved_id})

    def test_save_and_retrieve_object_should_delete_preceding_model_if_drop_option_is_True(self):
        # given
        db = self._get_testdb()
        model = RandomForestClassifier(max_depth=3, random_state=0)
        X = pd.DataFrame(data=[[1, 2], [3, 4], [11, 3]], columns=['a', 'b'])
        test = pd.DataFrame(data=[[2, 2], [1, 1]], columns=['a', 'b'])
        y = [3, 7, 14]
        model.fit(X, y)

        # when
        db.save_object(model, 't_test_models', 'my_model')
        db.save_object(model, 't_test_models', 'my_model', drop_if_exists=True)
        _, _ = db.retrieve_object(model_table='t_test_models', name='my_model')

        # then

    def test_get_column_names(self):
        # given
        db = self._get_testdb()
        table_name = 'my_table'
        db.engine.execute('create table %s (c1 INTEGER, c2 TEXT)' % table_name)

        # when
        result = db.get_column_names('%s' % table_name)

        # then
        assert_contains_all(result, ['c1', 'c2'])

    def test_drop_table_should_remove_table_from_db_if_exists(self):
        # given
        db = self._get_testdb()
        table_name = 'my_table'
        db.engine.execute('create table %s (c1 INTEGER, c2 TEXT)' % table_name)

        # when
        db.drop_table(table_name)
        db.drop_table(table_name)

        # then
        metadata = MetaData(db.engine, reflect=True)
        self.assertFalse(metadata.tables.__contains__(table_name))

    def test_execute_should_call_sqlalchemy_engine_execute_func(self):
        # given
        with patch('dd.DB.engine', new_callable=PropertyMock) as engine:
            mock = MagicMock()
            mock.execute = lambda x: x
            engine.return_value = mock
            db = self._get_testdb()

            # when
            result = db.execute('create table test (c1 INTEGER, c2 TEXT);')

        # then
        self.assertEqual(result, 'create table test (c1 INTEGER, c2 TEXT);')

    def test_sqlname_format(self):
        self.assertTrue(check_and_format_sqlname("col12_a"))
        self.assertFalse(check_and_format_sqlname("0"))
        self.assertFalse(check_and_format_sqlname("0colname"))
        self.assertFalse(check_and_format_sqlname("A0_12"))
        self.assertFalse(check_and_format_sqlname("A0-12"))
        self.assertFalse(check_and_format_sqlname("MyColumnName"))
        self.assertTrue(check_and_format_sqlname("myschema.mytable"))
        self.assertFalse(check_and_format_sqlname("myschema.."))
        self.assertFalse(check_and_format_sqlname("myschema."))
        self.assertFalse(check_and_format_sqlname("myschema._a"))
        self.assertFalse(check_and_format_sqlname("myschema.1"))

    def test_normalize_column_name(self):
        self.assertEqual(normalize_column_name(12), "c12")
        self.assertEqual(normalize_column_name("toto /é"), "toto")
        self.assertEqual(normalize_column_name("!/.column@"), "column")
        self.assertEqual(normalize_column_name("0toto"), "c0toto")
        self.assertEqual(normalize_column_name("MajU_2"), "maju_2")

    def test_import_with_num_colnames_must_raise(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(np.random.randn(10, 5))

        # when
        with self.assertRaises(ValueError) as ctx:
            result = db.import_dataframe(df, 'titanic.train', if_exists='replace')

        # then
        self.assertTrue(str(ctx.exception).startswith("Invalid column names"))

    def test_import_with_sqlite_memory_and_fastmode_must_raise(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(np.random.randn(10, 5))
        df.columns = map(lambda x: "num_" + str(x), df.columns)

        # when
        result = db.import_dataframe(df, 'titanic.train', if_exists='replace', fast_mode=True)

        # then
        self.assertEqual(result, 'titanic.train')

    def test_import_with_fastmode_should_have_same_behavior_without_fastmode(self):
        # given
        with tempfile.NamedTemporaryFile() as tmp:
            db = DB(dbtype='sqlite', filename=tmp.name)
            df = pd.DataFrame(np.random.randn(10, 5))
            df.columns = map(lambda x: "num_" + str(x), df.columns)

            # when
            db.import_dataframe(df, 'titanic_train_fast', if_exists='replace', fast_mode=True)
            result_fast = pd.read_sql('titanic_train_fast', con=db.engine)
            db.import_dataframe(df, 'titanic_train', if_exists='replace', fast_mode=False)
            result_slow = pd.read_sql('titanic_train_fast', con=db.engine)

            # then
            pd.testing.assert_frame_equal(result_fast, result_slow)

    def test_import_dataframe_with_index_param_should_warn(self):
        # given
        db = self._get_testdb()
        df = pd.DataFrame(columns=['c1'])
        table_name = 'table_name'

        # when
        with patch('warnings.warn') as warn_mock:
            result = db.import_dataframe(df, table_name, index=True)

            # then
            # TODO adapt the following line when dependency to db.py has been removed
            warn_mock.assert_called_once()

            result_error = warn_mock.call_args[0][0]
            self.assertIsInstance(result_error, DeprecationWarning)
            self.assertEqual(str(result_error), '"index" parameter is not supported anymore')

    def test_load_info_from_profile_null_should_raise(self):
        # given
        properties = {
            'dbname': 'mydb',
            'dbtype': 'mytype',
            'hostname': 'myhost',
            'password': 'mypass',
            'port': 3115,
            'username': 'myuser',
            'filename': 'myfile'
        }
        db = DB(**properties)

        # then
        with self.assertRaises(Exception):
            # when
            db._load_info_from_profile()

    def test_init_with_profile_should_instantiate_class_fields_with_airflow_connection_parameters(self):
        # given
        properties = {
            'dbname': 'mydb',
            'dbtype': 'mytype',
            'hostname': 'myhost',
            'password': 'mypass',
            'port': 3115,
            'username': 'myuser',
            'filename': 'mydb'
        }

        # when
        conn_mock = MagicMock()
        conn_mock.schema = properties['dbname']
        conn_mock.conn_type = properties['dbtype']
        conn_mock.host = properties['hostname']
        conn_mock.password = properties['password']
        conn_mock.port = properties['port']
        conn_mock.login = properties['username']

        with patch('dd.ddhook.DDHook.get_connection', return_value=conn_mock):
            db = DB(profile='test_profile')

        # then
        for field, value in six.iteritems(properties):
            self.assertEqual(getattr(db, field), value)

    def test_init_should_raise_exception_when_dtype_and_profile_args_areEqualTo_None(self):
        # then
        with self.assertRaises(Exception):
            DB(dbtype=None, profile=None)

    def _get_testdb(self):
        db = DB(dbtype='sqlite', filename=':memory:')
        return db

    def test_init_with_profile_should_set_filename_to_connection_schema_when_dbtype_is_sqlite(self):
        # Given
        conn_mock = MagicMock()
        conn_mock.conn_type = 'sqlite'
        conn_mock.schema = 'test_schema'

        # When
        with patch('dd.ddhook.DDHook.get_connection', return_value=conn_mock):
            db = DB(profile='test_profile')

        # Then
        self.assertIsNotNone(db.filename)

    def test_ensure_schema_should_create_schema_sql_statment_when_schema_is_not_none_and_dbtype_is_postgres_str_in_unicode(
            self):
        # # Given
        db = self._get_testdb()
        db.dbtype = u'postgres'

        with patch('dd.DB.engine') as engine:
            # When
            db._ensure_schema('schematocreate')

            # Then
            engine.execute.assert_called_with('CREATE SCHEMA IF NOT EXISTS schematocreate')


class TableMatcher(object):
    def __init__(self, expected):
        self.expected = expected

    def __eq__(self, other):
        if isinstance(other, Table):
            return (other.name == self.expected.name)
        return False


if __name__ == '__main__':
    unittest.main()
