from dd.api.workflow.callables import LoadFileCallableBuilder, \
    LoadDataFrameCallableBuilder

from dd.api.workflow import dataset

from dd.api.workflow.readers import CSVReader


class DatasetReader:
    def __init__(self, context):
        self.context = context

    def table(self, table_name):
        return self.context.table(table_name)

    def csv(self, filepath, output_table, write_options=None, normalize=False,
            **read_options):
        reader = CSVReader(**read_options)
        return self.context.load_file(filepath, output_table,
                                      write_options=write_options,
                                      normalize=normalize,
                                      reader=reader)

    def dataframe(self, dataframe, output_table, **write_options):
        callable_builder = (LoadDataFrameCallableBuilder(self.context)
                            .with_dataframe(dataframe)
                            .with_write_options(**write_options))

        return dataset.DatasetLoad(self.context, callable_builder, output_table)
