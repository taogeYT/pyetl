# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
from abc import ABC, abstractmethod

from pydbclib import connect

from pyetl.dataset import Dataset


class Reader(ABC):

    @abstractmethod
    def read(self, columns):
        pass


class DatabaseReader(Reader):

    def __init__(self, uri, table_name, condition=None, limit=None):
        self.db = connect(uri)
        self.table_name = table_name
        self.condition = condition if condition else "1=1"
        self.limit = limit

    def _read_dataset(self, text):
        return Dataset((r for r in self.db.read(text)))

    def _query_text(self, columns):
        fields = [f"{col} as {alias}" for col, alias in columns.items()]
        return " ".join(["select", ",".join(fields), "from", self.table_name])

    def read(self, columns):
        text = self._query_text(columns)
        if isinstance(self.condition, str):
            text = f"{text} where {self.condition}"
            dataset = self._read_dataset(text)
        elif callable(self.condition):
            dataset = self._read_dataset(text).filter(self.condition)
        else:
            raise ValueError("condition 参数错误")
        if isinstance(self.limit, int):
            return dataset.limit(self.limit)
        else:
            return dataset


class FileReader(Reader):

    def __init__(self, file_path, sep=','):
        self.file_path = file_path
        self.sep = sep

    def read(self, columns):
        def get_record(reader):
            for df in reader:
                df = df.rename(columns=columns)
                for record in df.to_dict("records"):
                    yield record
        import pandas
        file_reader = pandas.read_csv(self.file_path, sep=self.sep, chunksize=10000)
        return Dataset(get_record(file_reader))


class ExcelReader(Reader):

    def __init__(self, file_path, sheet_name=0):
        self.file_path = file_path
        self.sheet_name = sheet_name

    def read(self, columns):
        import pandas
        df = pandas.read_excel(self.file_path, sheet_name=self.sheet_name).rename(columns=columns)
        return Dataset(df.to_dict("records"))
