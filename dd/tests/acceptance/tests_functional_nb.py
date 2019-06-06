#-*- coding: utf-8 -*-
import io
import os
import unittest

import pytest

pytestmark = pytest.mark.functional

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors.execute import CellExecutionError

@pytest.mark.functional
class TestFunctional(unittest.TestCase):

    def setUp(self):

        self.basedir = os.path.dirname(os.path.abspath(__file__))

        self.run_path = '{}/'.format(self.basedir)
        self.output_dir = '{}/out'.format(self.basedir)
        self.notebook_filename_out = '{}/out/TestOutput_{}.ipynb'
        self.notebook_base_url = '{}/getting_started/{}.ipynb'

        if not os.path.isdir(self.output_dir):
            os.mkdir(self.output_dir)

    def test_tutorial_0_DatadriverForDataScientists_Part1_should_execute_whithout_error(self):
        # Given
        notebook_name = '0-DatadriverForDataScientists_Part1'

        # When
        self.execute_notebook(notebook_name)

    def test_tutorial_1_DatadriverForDataScientists_Part2_should_execute_whithout_error(self):
        # Given
        notebook_name = '1-DatadriverForDataScientists_Part2'

        # When
        self.execute_notebook(notebook_name)

    def test_tutorial_2_AdvancedDataDriver_Caching_should_execute_whithout_error(self):
        # Given
        notebook_name = '2-AdvancedDataDriver_Caching'

        # When
        self.execute_notebook(notebook_name)

    def test_tutorial_3_AdvancedDatadriver_WriteOptions_should_execute_whithout_error(self):
        # Given
        notebook_name = '3-AdvancedDatadriver_WriteOptions'

        # When
        self.execute_notebook(notebook_name)

    def test_tutorial_4_AdvancedDatadriver_SQL_should_execute_whithout_error(self):
        # Given
        notebook_name = '4-AdvancedDatadriver_SQL'

        # When
        self.execute_notebook(notebook_name)

    def test_tutorial_TestsAcceptance_Reader_should_execute_whithout_error(self):
        # Given
        notebook_name = 'TestsAcceptance_Reader'

        # When
        self.execute_notebook(notebook_name)


    def execute_notebook(self, notebook_name):
        with io.open(self.notebook_base_url.format(self.basedir, notebook_name), encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
            ep = ExecutePreprocessor(timeout=1000)

            try:
                out = ep.preprocess(nb, {'metadata': {'path': self.run_path}})
            except CellExecutionError:
                out = None
                msg = 'Error executing the notebook "%s".\n\n' % notebook_name
                msg += 'See notebook "%s" for the traceback.' % self.notebook_filename_out
                print(msg)
                raise
            finally:
                with io.open(self.notebook_filename_out.format(self.basedir, notebook_name), mode='wt', encoding='utf-8') as f:
                    nbformat.write(nb, f)