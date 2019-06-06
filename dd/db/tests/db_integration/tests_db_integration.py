import os
import shutil
from unittest import TestCase

import pandas as pd
from airflow import settings, models
from airflow.utils.db import merge_conn, initdb, resetdb
from dd import DBArrow
from pandas.util.testing import assert_frame_equal
from sklearn.ensemble import RandomForestClassifier

TMP_TEST_PATH = os.path.abspath('/tmp/ddapi_test')


class TestIntegrationDBArrow(TestCase):
    """
    This test class requires a Minio server which its hostname is 'minio'
    Typically launched by the CI

    To run it on local, execute the following command, before :
        docker run -p 9000:9000 -e "MINIO_ACCESS_KEY=key_test" -e "MINIO_SECRET_KEY=secret_test" minio/minio server /data
    """

    def __init__(self, methodName):
        super(TestIntegrationDBArrow, self).__init__(methodName=methodName)
        self.addTypeEqualityFunc(RandomForestClassifier, self.assert_rf_classifier_equal)

    def setUp(self):
        self.bucket = 'mydata'

        # create a new aiflow connection to the minio server with the url http://${minio_host}:9000
        minio_host = os.environ.get('MINIO_SERVER','localhost')
        minio_access_key = os.environ.get('MINIO_ACCESS_KEY','key_test')
        minio_secret_key = os.environ.get('MINIO_SECRET_KEY','secret_test')

        self.session = settings.Session()
        self.profile = 'test_integration_minio'
        initdb()
        self._remove_profile()
        merge_conn(
            models.Connection(
                conn_id=self.profile, conn_type='s3',
                extra='{' +
                      f'"aws_access_key_id":"{minio_access_key}",' +
                      f'"aws_secret_access_key":"{minio_secret_key}",' +
                      f'"host":"http://{minio_host}:9000",' +
                      '"region_name":""' +
                      '}'
            )
        )
        self.session.commit()
        if not os.path.exists(TMP_TEST_PATH):
            os.mkdir(TMP_TEST_PATH)
        else:
            shutil.rmtree(TMP_TEST_PATH)
            os.mkdir(TMP_TEST_PATH)

        self.db = DBArrow(profile=self.profile, bucket=self.bucket)
        if not self.db.get_conn().bucket_exists(self.bucket):
            self.db.get_conn().make_bucket(self.bucket)

    def tearDown(self):
        self._remove_profile()
        self.session.close()

    def test_dbarrow_interactions_with_minio_server(self):
        # given
        df = pd.DataFrame(data=[[1, 2, 3], [1, 1, 2]], columns=['c1', 'c2', 'c3'])

        obj = RandomForestClassifier()
        obj.fit(df[['c1', 'c2']], df['c3'])
        metadata = {
            'model_id': 42,
            'vector_psquare': [1.04, 2.05, 3.06]
        }

        # when
        self.db.import_dataframe(df, 'prefix/to/master.arrow')
        result_df = self.db.retrieve_table('prefix/to/master.arrow')

        self.db.save_object(obj, 'prefix/to', 'rf.pickle', metadata=metadata)
        result_obj, result_metadata = self.db.retrieve_object('prefix/to', 'rf.pickle')

        # then
        self.assertTrue(self.db.get_conn().bucket_exists('mydata'))
        assert_frame_equal(df, result_df)
        self.assert_rf_classifier_equal(obj, result_obj)
        self.assertEqual(metadata, result_metadata)

    def test_dbarrow_interactions_with_localFS(self):
        # given
        db = DBArrow()
        df = pd.DataFrame(data=[[1, 2, 3], [1, 1, 2]], columns=['c1', 'c2', 'c3'])

        obj = RandomForestClassifier()
        obj.fit(df[['c1', 'c2']], df['c3'])
        metadata = {
            'model_id': 42,
            'vector_psquare': [1.04, 2.05, 3.06]
        }

        # when
        db.import_dataframe(df, '{}/master.arrow'.format(TMP_TEST_PATH))
        result_df = db.retrieve_table('{}/master.arrow'.format(TMP_TEST_PATH))

        db.save_object(obj, '/tmp/ddapi_test', 'rf.pickle', metadata=metadata)
        result_obj, result_metadata = db.retrieve_object('/tmp/ddapi_test', 'rf.pickle')

        # then
        assert_frame_equal(df, result_df)
        self.assert_rf_classifier_equal(obj, result_obj)
        self.assertEqual(metadata, result_metadata)

    def test_dbarrow_interactions_with_localFS_and_PARQUET_format(self):
        # given
        db = DBArrow(format='parquet')
        df = pd.DataFrame(data=[[1, 2, 3], [1, 1, 2]], columns=['c1', 'c2', 'c3'])


        # when
        db.import_dataframe(df, '{}/master.parquet'.format(TMP_TEST_PATH), compression='gzip')
        result_df = db.retrieve_table('{}/master.parquet'.format(TMP_TEST_PATH), columns=['c2', 'c3'])

        # then
        assert_frame_equal(df[['c2', 'c3']], result_df)

    def test_dbarrow_interactions_with_minio_server_and_PARQUET_format(self):
        # given
        self.db.format = 'parquet'
        df = pd.DataFrame(data=[[1, 2, 3], [1, 1, 2]], columns=['c1', 'c2', 'c3'])


        # when
        self.db.import_dataframe(df, 'prefix/to//master.parquet', compression='gzip')
        result_df = self.db.retrieve_table('prefix/to//master.parquet', columns=['c2', 'c3'])

        # then
        assert_frame_equal(df[['c2', 'c3']], result_df)


    def assert_rf_classifier_equal(self, first, second, msg='{} and {} are not equal'):
        if type(first) is not type(second):
            raise self.failureException('''{} 
                                            and 
                                            {} 
                                            are not object from the same type'''.format(type(first), type(second)))

        if str(first) != str(second):
            raise self.failureException(msg.format(first, second))

    def _remove_profile(self):
        self.session.execute("delete from connection where conn_id='{}'".format(self.profile))
        self.session.commit()