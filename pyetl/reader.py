# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
from abc import ABC, abstractmethod

import pandas
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

    def __init__(self, file_path, pd_params=None):
        self.file_path = file_path
        if pd_params is None:
            pd_params = {}
        pd_params.setdefault("chunksize", 10000)
        self.file = pandas.read_csv(self.file_path, **pd_params)
        self.df = self.file.read(0)

    def _get_records(self, columns):
        for df in self.file:
            df = df.where(df.notnull(), None).rename(columns=columns)
            for record in df.to_dict("records"):
                yield record

    def read(self, columns):
        return Dataset(self._get_records(columns))


class ExcelReader(Reader):

    def __init__(self, file, sheet_name=0, pd_params=None):
        self.sheet_name = sheet_name
        if isinstance(file, str):
            self.file = pandas.ExcelFile(self.file)
        elif isinstance(file, pandas.ExcelFile):
            self.file = file
        if pd_params is None:
            pd_params = {}
        self.df = self.file.parse(self.sheet_name, **pd_params)

    def read(self, columns):
        df = self.df.where(self.df.notnull(), None).rename(columns=columns)
        return Dataset(df.to_dict("records"))
