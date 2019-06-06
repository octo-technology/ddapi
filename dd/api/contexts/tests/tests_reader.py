import unittest

import pandas as pd
from mock import MagicMock, patch

from dd.api.contexts import LocalContext
from dd.api.contexts.reader import DatasetReader
from dd.api.workflow.callables import LoadDataFrameCallableBuilder
from dd.api.workflow.dataset import Dataset, DatasetLoad


class TestLocalContext(unittest.TestCase):
    def setUp(self):
        # Given
        db = MagicMock()
        db.retrieve_table.return_value = MagicMock(spec_set=pd.DataFrame)
        context = LocalContext(db)
        reader = DatasetReader(context)
        self.reader = reader

    def test_datasetreader_table_should_return_dataset(self):
        # Given
        reader = self.reader

        # When
        dataset = reader.table("table_name")

        # Check
        self.assertIsInstance(dataset, Dataset)

    def test_datasetreader_csv_should_return_dataset(self):
        # Given
        reader = self.reader

        # When
        dataset = reader.csv("filepath", "output_table")

        # Check
        self.assertIsInstance(dataset, Dataset)

    def test_datasetreader_should_pass_options_to_dataset(self):
        # Given
        mock = MagicMock()
        reader = self.reader
        reader.context = mock

        # When
        _ = reader.csv("filepath", "output_table", sep=',', header=None)

        # Check
        args, kwargs = mock.load_file.call_args
        reader = kwargs.get('reader', {})
        self.assertEqual(reader.read_options['sep'], ',')
        self.assertEqual(reader.read_options['header'], None)

    def test_dataframe_should_return_dataset(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(10, 10),
                                 columns=list("abcdefghij"))
        reader = self.reader
        output_table = "my_output_table"

        # When
        result = reader.dataframe(dataframe, output_table)

        # Check
        self.assertIsInstance(result, Dataset)

    def test_dataframe_should_pass_output_table_parameter(self):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(10, 10),
                                 columns=list("abcdefghij"))
        reader = self.reader
        output_table = "my_output_table"

        # When
        result = reader.dataframe(dataframe, output_table)

        # Check
        self.assertEqual(result.output_table, output_table)

    @patch("dd.api.workflow.dataset.DatasetLoad")
    def test_dataframe_should_create_dataset_with_correct_callable_builder(self, datasetload_mock):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(10, 10),
                                 columns=list("abcdefghij"))
        reader = self.reader
        output_table = "my_output_table"

        # When
        _ = reader.dataframe(dataframe, output_table)

        # Check
        args, kwargs = datasetload_mock.call_args
        builder = args[1]
        self.assertIsInstance(builder, LoadDataFrameCallableBuilder)

    @patch("dd.api.workflow.dataset.DatasetLoad")
    def test_dataframe_should_create_dataset_with_correct_callable_builder(self, datasetload_mock):
        # Given
        import numpy as np
        import pandas as pd

        dataframe = pd.DataFrame(np.random.rand(10, 10),
                                 columns=list("abcdefghij"))
        reader = self.reader
        output_table = "my_output_table"

        # When
        _ = reader.dataframe(dataframe, output_table, index=False, if_exists='replace')

        # Check
        args, kwargs = datasetload_mock.call_args
        builder = args[1]
        self.assertIsInstance(builder, LoadDataFrameCallableBuilder)
        self.assertEqual(builder.write_options,
                         dict(if_exists="replace", index=False))
