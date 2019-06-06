import io
import json
import os
import pickle
from six.moves.urllib import parse

import pyarrow as pa
from airflow.hooks.S3_hook import S3Hook
from minio import Minio

CHUNK_SIZE = 32 * 1024


class DBArrow(S3Hook):
    """
    Wrapper class of the Apache Arrow data format.
    This class implement the interface DB from datadriver's ddapi.
    """

    def __init__(self, profile=None, bucket=None, format='arrow'):
        """
        Creates a specialized S3Hook for Arrow using Minio interface.
        Supports any Minio supported blob store (see https://docs.minio.io/docs/python-client-api-reference)
        Setup Blob Store : you need to create an Airflow's connection with the "S3 Conn Type"

        Parameters
        ----------
        :param profile: The Airflow connection id; if None, it uses the local FileSystem
        :param bucket: The S3 / blob store bucket to read from /write to; required if profile is set
        :param format: The serialization format used to read/write dataframe. Default to 'arrow'. Supported values are 'arrow', 'parquet"
        :type profile: str
        :type bucket: str
        :type format: str
        """
        if profile is not None:
            super(DBArrow, self).__init__(aws_conn_id=profile)
            self.bucket = bucket

        self.format = format
        self.conn = None

    def import_dataframe(self, df, path_key, *args, **kwargs):
        """
        Serialize and store a pandas.DataFrame to a local file or to a S3 blob (minio).
        If Arrow/Parquet file exists, it will be override

        Parameters
        ----------
        :param df: The pandas.DataFrame you want to store.
        :param path_key: The absolute/relative filepath where it will be saved.
        :param args: List of ParquetWriter options like compression ['gzip", 'snappy', 'none']
        :param kwargs: Dictionary of named ParquetWriter options like compression ['gzip", 'snappy', 'none'] (see: https://arrow.apache.org/docs/python/parquet.html)
        :type df: pandas.DataFrame
        :type path_key: str
        :return: The table_name parameter value

        Examples
        --------
        >>> from dd import DBArrow
        >>> import pandas
        >>> df = pandas.DataFrame(np.random.randn(10, 5))
        >>> db = DBArrow()
        >>> db.import_dataframe(df, '/tmp/dataframe.arrow')

        >>> db = DBArrow(format='parquet')
        >>> db.import_dataframe(df, '/tmp/dataframe.parquet', compression='gzip')
        """
        df = self._object_columns_astype_str(df)

        if self.is_aws_conn_id_not_none:
            buf = io.BytesIO()
            if self.format == 'parquet':
                self._write_parquet_format(buf, df, *args, **kwargs)
            else:
                self._write_arrow_format(buf, df)
            self._upload_object(buf, path_key)
        else:
            if self.format == 'parquet':
                self._write_parquet_format(path_key, df, *args, **kwargs)
            else:
                with pa.OSFile(path_key, 'wb') as f:
                    self._write_arrow_format(f, df)

        return path_key

    def retrieve_object(self, prefix, key, *args, **kwargs):
        """
        Reads a Pickle from a local file or from a S3 blob (minio), then loads it as an instance to be returned

        Parameters
        ----------
        :param prefix: The path to access to the object (the prefix of the key)
        :param key: The file name and extension
        :type prefix: str
        :type key: str
        :return: The object instance and its metadata loaded from json to a python dictionary

        Examples
        --------
        >>> from dd import DBArrow
        >>> db = DBArrow(profile='s3_connection_id', bucket='mybucket')
        >>> model, metadata = db.retrieve_object('/prefix/to', 'my_model.pickle')
        """

        path_key = '{}/{}'.format(prefix, key)
        if self.is_aws_conn_id_not_none:
            with pa.BufferOutputStream() as buf:
                self._download_object(buf, path_key)
            obj = pickle.loads(buf.getvalue())
            with io.BytesIO() as meta_buf:
                self._download_object(meta_buf, path_key + '.meta')
                metadata = json.loads(meta_buf.getvalue())
            return obj, metadata
        else:
            with open(path_key, 'rb') as f:
                obj = pickle.load(f)

            metadata_filename = '{}.meta'.format(path_key)
            if os.path.exists(os.path.abspath(metadata_filename)):
                with open(metadata_filename, 'rt') as f:
                    metadata = json.loads(f.read())
            else:
                metadata = None

            return obj, metadata

    def retrieve_table(self, path_key, *args, **kwargs):
        """
        Reads an Arrow file locally or from a S3 blob (minio), and return it as a pandas.DataFrame.

        Parameters
        ----------
        :param path_key: The absolute/relative filepath it will read from.
        :param kwargs: Dictionary of named Parquet read options like columns=[] projection (see: https://arrow.apache.org/docs/python/parquet.html)
        :type path_key: str
        :return: The pandas.Dataframe

        Examples
        --------
        >>> from dd import DBArrow
        >>> db = DBArrow()
        >>> df = db.retrieve_table('/tmp/dataframe.arrow')

        >>> db = DBArrow(format='parquet')
        >>> df = db.retrieve_table('/tmp/dataframe.parquet', columns=[0, 1, 2, 3])
        """

        if self.is_aws_conn_id_not_none:
            with pa.BufferOutputStream() as buf:
                self._download_object(buf, path_key)
            if self.format == 'parquet':
                return self._read_parquet_format(pa.BufferReader(buf.getvalue()), *args, **kwargs)
            else:
                return self._read_arrow_format(buf.getvalue())
        else:
            with pa.memory_map(path_key, 'rb') as f:
                if self.format == 'parquet':
                    return self._read_parquet_format(f, *args, **kwargs)
                else:
                    return self._read_arrow_format(f)

    def save_object(self, obj, prefix, key, metadata=None, *args, **kwargs):
        """
        Save a Pickle to a local file or to a S3 blob (minio)

        Parameters
        ----------
        :param obj: The path to access to the object (the prefix of the key)
        :param prefix: The path to access to the object (the prefix of the key)
        :param key: The file name and extension
        :param metadata: A dictionary of metadata that will be stored in json as the blob metadata
        :type obj: object
        :type prefix: str
        :type key: str
        :type metadata: dict
        :return: None

        Examples
        --------
        >>> from dd import DBArrow
        >>> db = DBArrow(profile='s3_connection_id', bucket='mybucket')
        >>> obj = {'attr': 'any value'}
        >>> metadata = {'id': 42}
        >>> db.save_object(obj, '/prefix/to', 'my_model.pickle', metadata)
        """
        path_key = '{}/{}'.format(prefix, key)
        import json
        metadata_json = json.dumps(metadata)
        if self.is_aws_conn_id_not_none:
            buf = io.BytesIO()
            meta_buf = io.BytesIO(metadata_json.encode('utf-8'))
            pickle.dump(obj, buf, pickle.HIGHEST_PROTOCOL)
            self._upload_object(buf, path_key)
            self._upload_object(meta_buf, path_key + '.meta')
            return

        else:
            with open(path_key, 'wb') as f:
                pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
            with open('{}.meta'.format(path_key), 'wt') as meta_f:
                meta_f.write(metadata_json)

    def execute(self, *args, **kwargs):
        raise NotImplementedError()

    def read_sql(self, query):
        raise NotImplementedError()

    def get_conn(self):
        if self.conn is None:
            if self.aws_conn_id is not None:
                session, endpoint_url = self._get_credentials(region_name=None)
                self.conn = self._create_minio_conn_from_boto_session(session, endpoint_url)
        return self.conn

    def _create_minio_conn_from_boto_session(self, session, endpoint_url):
        creds = session.get_credentials()
        result = parse.urlparse(endpoint_url)
        region_name = session.region_name
        is_secure = True if result.scheme == 'https' else False
        return Minio(endpoint=result.netloc,
                     access_key=creds.access_key,
                     secret_key=creds.secret_key,
                     secure=is_secure,
                     region=region_name)

    def _object_columns_astype_str(self, df):
        import numpy
        for column_of_type_object in df.select_dtypes(include=[numpy.object]):
            df[column_of_type_object] = df[column_of_type_object].astype(str)
        return df

    def assert_bucket_provided(self):
        assert self.bucket is not None, 'bucket must be defined if S3 "like" connection is used'

    def _upload_object(self, buf, path_key, metadata=None):
        self.assert_bucket_provided()
        buf.seek(0)  # reset the offset pointer to start writing out from the beginning
        self.get_conn().put_object(self.bucket, path_key, buf, len(buf.getvalue()), metadata=metadata)

    def _download_object(self, buf, path_key):
        self.assert_bucket_provided()
        resp = self.get_conn().get_object(self.bucket, path_key)
        for chunk_of_data in resp.stream(CHUNK_SIZE):
            buf.write(chunk_of_data)
        resp.close()

    def _write_arrow_format(self, buf, df):
        table = pa.Table.from_pandas(df)
        writer = pa.RecordBatchFileWriter(buf, table.schema)
        writer.write_table(table)
        writer.close()

    def _write_parquet_format(self, path_key, df, *args, **kwargs):
        from pyarrow import parquet
        table = pa.Table.from_pandas(df)
        writer = parquet.ParquetWriter(path_key, table.schema, *args, **kwargs)
        writer.write_table(table)
        writer.close()

    def _read_arrow_format(self, buf):
        reader = pa.RecordBatchFileReader(buf)
        df = reader.read_pandas()
        return df

    def _read_parquet_format(self, buf, *args, **kwargs):
        from pyarrow import parquet
        df = parquet.read_pandas(buf, *args, **kwargs).to_pandas()
        return df

    @property
    def is_aws_conn_id_not_none(self):
        return getattr(self, "aws_conn_id", None) is not None
