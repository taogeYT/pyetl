# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
from abc import ABC, abstractmethod

import pandas

from pyetl.connections import DatabaseConnection, ElasticsearchConnection
from pyetl.dataset import Dataset


class Reader(ABC):
    _columns = None

    @abstractmethod
    def read(self, columns):
        """返回结果列名必须rename"""
        pass

    @property
    @abstractmethod
    def columns(self):
        return self._columns


class DatabaseReader(DatabaseConnection, Reader):

    def __init__(self, db, table_name, condition=None, limit=None):
        super().__init__(db)
        self.table_name = table_name
        self.table = self.db.get_table(self.table_name)
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
            raise ValueError("condition 参数类型错误")
        if isinstance(self.limit, int):
            return dataset.limit(self.limit)
        else:
            return dataset

    @property
    def columns(self):
        if self._columns is None:
            self._columns = self.db.get_table(self.table_name).get_columns()
        return self._columns


class FileReader(Reader):

    def __init__(self, file_path, pd_params=None):
        self.file_path = file_path
        if pd_params is None:
            pd_params = {}
        pd_params.setdefault("chunksize", 10000)
        self.file = pandas.read_csv(self.file_path, **pd_params)

    def _get_records(self, columns):
        for df in self.file:
            df = df.where(df.notnull(), None).rename(columns=columns)
            for record in df.to_dict("records"):
                yield record

    def read(self, columns):
        return Dataset(self._get_records(columns))

    @property
    def columns(self):
        if self._columns is None:
            self._columns = [col for col in self.file.read(0).columns]
        return self._columns


class ExcelReader(Reader):

    def __init__(self, file, sheet_name=0, pd_params=None, detect_table_border=True):
        if pd_params is None:
            pd_params = {}
        pd_params.setdefault("dtype", 'object')
        self.sheet_name = sheet_name
        if isinstance(file, str):
            file = pandas.ExcelFile(file)
            self.df = file.parse(self.sheet_name, **pd_params)
        elif isinstance(file, pandas.ExcelFile):
            self.df = file.parse(self.sheet_name, **pd_params)
        elif isinstance(file, pandas.DataFrame):
            self.df = file
        else:
            raise ValueError(f"file 参数类型错误")
        if detect_table_border:
            self.detect_table_border()

    def read(self, columns):
        df = self.df.where(self.df.notnull(), None).rename(columns=columns)
        return Dataset(df.to_dict("records"))

    @property
    def columns(self):
        if self._columns is None:
            self._columns = [col for col in self.df.columns]
        return self._columns

    def detect_table_border(self):
        y, x = self.df.shape
        axis_x = self.df.count()
        for i in range(axis_x.size):
            name = axis_x.index[i]
            count = axis_x.iloc[i]
            if isinstance(name, str) and name.startswith("Unnamed:") and count == 0:
                x = i
                break
        axis_y = self.df.count(axis=1)
        for i in range(axis_y.size):
            count = axis_y.iloc[i]
            if count == 0:
                y = i
                break
        self.df = self.df.iloc[:y, :x]


class ElasticsearchReader(ElasticsearchConnection, Reader):

    def __init__(self, index_name, doc_type=None, es_params=None, batch_size=10000):
        super().__init__(es_params)
        self.index_name = index_name
        self.doc_type = doc_type
        self.batch_size = batch_size
        self.index = self.client.get_index(self.index_name, self.doc_type)

    def read(self, columns):
        return Dataset(doc["_source"] for doc in self.index.scan()).rename_and_extract(columns)

    @property
    def columns(self):
        if self._columns is None:
            self._columns = self.index.get_columns()
        return self._columns
