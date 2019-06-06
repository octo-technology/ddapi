import pandas as pd


class CSVReader(object):
    def __init__(self, **read_options):
        self.read_options = read_options

    def read(self, path):
        return pd.read_csv(path, **self.read_options)