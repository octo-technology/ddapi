from abc import abstractmethod, abstractproperty


class AbstractDataset(object):
    @abstractmethod
    def __getitem__(self, item):
        raise NotImplementedError

    @abstractmethod
    def head(self, n):
        raise NotImplementedError

    @abstractproperty
    def columns(self):
        raise NotImplementedError

    @abstractproperty
    def shape(self):
        raise NotImplementedError

    @abstractmethod
    def collect(self):
        raise NotImplementedError

    @abstractmethod
    def join(self, other):
        raise NotImplementedError

    @abstractmethod
    def select_columns(self, columns, output_table=None):
        raise NotImplementedError

    @abstractmethod
    def split_train_test(self, function):
        raise NotImplementedError

    @abstractmethod
    def transform(self, function):
        raise NotImplementedError


class AbstractModel(object):
    @abstractmethod
    def predict(self, model, target):
        raise NotImplementedError

    @abstractmethod
    def transform(self, dataset):
        raise NotImplementedError

    @abstractmethod
    def collect(self):
        raise NotImplementedError
