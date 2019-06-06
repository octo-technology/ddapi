import unittest

import pandas as pd
from airflow.models import Connection
from minio import Object
from mock import MagicMock, patch, ANY

from dd import DBArrow


class ToPickleDumbClass:
    attr = 'a class attr'


class TestDBArrowAdapter(unittest.TestCase):

    def test_read_sql_should_raise_NotImplementedError(self):
        # given
        db = DBArrow()

        # when
        with self.assertRaises(NotImplementedError) as context:
            db.read_sql(None)

    def test_execute_should_raise_NotImplementedError(self):
        # given
        db = DBArrow()

        # when
        with self.assertRaises(NotImplementedError) as context:
            db.execute()

    def test_get_conn_should_return_minio_client_with_aws_creds_from_airflow_connection_when_profile_is_set(self):
        # given
        profile = 'test_conn_id'
        con = Connection(conn_id='test_conn_id',
                         extra='{"aws_access_key_id": "key",' +
                               '"aws_secret_access_key": "secret",' +
                               '"region_name": "region1",' +
                               '"host": "http://minio.hostname:8383"}'
                         )
        db = DBArrow(profile=profile)

        # when
        with patch('airflow.hooks.base_hook.BaseHook.get_connection', return_value=con) as get_connection:
            result = db.get_conn()

        # then
        get_connection.assert_called_once_with('test_conn_id')
        self.assertEqual('key', result._access_key)
        self.assertEqual('secret', result._secret_key)
        self.assertEqual('region1', result._region)
        self.assertEqual(False, result._is_ssl)
        self.assertEqual('http://minio.hostname:8383', result._endpoint_url)

    def test_get_conn_should_return_minio_client_with_secure_as_True_when_url_start_with_https(self):
        # given
        profile = 'test_conn_id'
        con = Connection(conn_id='test_conn_id',
                         extra='{"aws_access_key_id": "key",' +
                               '"aws_secret_access_key": "secret",' +
                               '"region_name": "region1",' +
                               '"host": "https://minio.hostname:8383"}'
                         )
        blob_file = '/prefix/test.arrow'

        # when
        with patch('airflow.hooks.base_hook.BaseHook.get_connection', return_value=con) as get_connection:
            result = DBArrow(profile=profile).get_conn()

        # then
        self.assertEqual(True, result._is_ssl)

    def test_download_object_should_raise_assertion_error_when_bucket_is_none_and_aws_conn_is_not_none(self):
        # given
        db = DBArrow()
        db.aws_conn_id = MagicMock()
        db.bucket = None

        # then
        with self.assertRaises(AssertionError) as e:
            # when
            db._download_object(None, None)

    def test_object_columns_astype_str_should_convert_columns_of_dtype_object_to_str(self):
        # given
        db = DBArrow()
        df_with_column_type_object = pd.DataFrame(data=[['yo', 'ya'], [11, 22]], columns=['c1', 'c2'])

        # when
        result = db._object_columns_astype_str(df_with_column_type_object)

        # then it raises no error
        self.assertEqual('11', result['c1'][1])
        self.assertEqual('22', result['c2'][1])

    def test_upload_object_should_raise_assertion_error_when_bucket_is_none_and_aws_conn_is_not_none(self):
        # given
        db = DBArrow()
        db.aws_conn_id = MagicMock()
        db.bucket = None

        # then
        with self.assertRaises(AssertionError) as e:
            # when
            db._upload_object(None, None)

    def test_import_dataframe_should_call_write_arrow_format_by_default(self):
        # given
        db = DBArrow()
        db_minio = DBArrow(profile='aws_conn', bucket='mybucket')
        df = MagicMock()
        write_arrow = MagicMock()
        write_arrow_minio = MagicMock()
        write_parquet = MagicMock()
        buf = MagicMock()

        # when
        with patch('dd.db_arrow.DBArrow._write_arrow_format', write_arrow):
            with patch('dd.db_arrow.DBArrow._write_parquet_format', write_parquet):
                with patch('pyarrow.OSFile', buf):
                    db.import_dataframe(df, None)

        with patch('dd.db_arrow.DBArrow._write_arrow_format', write_arrow_minio):
            with patch('dd.db_arrow.DBArrow._write_parquet_format', write_parquet):
                with patch('dd.db_arrow.DBArrow._upload_object', MagicMock()):
                    db_minio.import_dataframe(df, None)

        # then
        write_arrow.assert_called_with(ANY, df)
        write_arrow_minio.assert_called_with(ANY, df)
        write_parquet.assert_not_called()

    def test_import_dataframe_should_call_write_parquet_format_when_dbarrow_is_initialize_with_fomat_param_equal_to_parquet(self):
        # given
        db = DBArrow(format='parquet')
        db_minio = DBArrow(profile='aws_conn', bucket='mybucket', format='parquet')
        df = MagicMock()
        write_parquet = MagicMock()
        write_parquet_minio = MagicMock()
        write_arrow = MagicMock()
        buf = MagicMock()

        # when
        with patch('dd.db_arrow.DBArrow._write_parquet_format', write_parquet):
            with patch('dd.db_arrow.DBArrow._write_arrow_format', write_arrow):
                with patch('pyarrow.parquet.ParquetFile', buf):
                    db.import_dataframe(df, None)

        with patch('dd.db_arrow.DBArrow._write_parquet_format', write_parquet_minio):
            with patch('dd.db_arrow.DBArrow._write_arrow_format', write_arrow):
                with patch('dd.db_arrow.DBArrow._upload_object', MagicMock()):
                    db_minio.import_dataframe(df, None)

        # then
        write_parquet.assert_called_with(ANY, df)
        write_parquet_minio.assert_called_with(ANY, df)
        write_arrow.assert_not_called()

    def test_retrieve_table_should_call_read_arrow_format_by_default(self):
        # given
        db = DBArrow()
        db_minio = DBArrow(profile='aws_conn', bucket='mybucket')
        df = MagicMock()
        read_arrow = MagicMock()
        read_arrow_minio = MagicMock()
        read_parquet = MagicMock()
        buf = MagicMock()

        # when
        with patch('dd.db_arrow.DBArrow._read_arrow_format', read_arrow):
            with patch('dd.db_arrow.DBArrow._read_parquet_format', read_parquet):
                with patch('pyarrow.memory_map', buf):
                    db.retrieve_table(None)

        with patch('dd.db_arrow.DBArrow._read_arrow_format', read_arrow_minio):
            with patch('dd.db_arrow.DBArrow._read_parquet_format', read_parquet):
                with patch('dd.db_arrow.DBArrow._download_object', MagicMock()):
                    db_minio.retrieve_table(None)

        # then
        read_arrow.assert_called_with(ANY)
        read_arrow_minio.assert_called_with(ANY)
        read_parquet.assert_not_called()

    def test_retrieve_table_should_call_read_parquet_format_when_dbarrow_is_initialize_with_fomat_param_equal_to_parquet(self):
        # given
        db = DBArrow(format='parquet')
        db_minio = DBArrow(profile='aws_conn', bucket='mybucket', format='parquet')
        df = MagicMock()
        read_parquet = MagicMock()
        read_parquet_minio = MagicMock()
        read_arrow = MagicMock()
        buf = MagicMock()

        # when
        with patch('dd.db_arrow.DBArrow._read_arrow_format', read_arrow):
            with patch('dd.db_arrow.DBArrow._read_parquet_format', read_parquet):
                with patch('pyarrow.memory_map', buf):
                    db.retrieve_table(None)

        with patch('dd.db_arrow.DBArrow._read_arrow_format', read_arrow):
            with patch('dd.db_arrow.DBArrow._read_parquet_format', read_parquet_minio):
                with patch('dd.db_arrow.DBArrow._download_object', MagicMock()):
                    db_minio.retrieve_table(None)

        # then
        read_parquet.assert_called_with(ANY)
        read_parquet_minio.assert_called_with(ANY)
        read_arrow.assert_not_called()
