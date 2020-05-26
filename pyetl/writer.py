# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
import os
import random
from abc import ABC, abstractmethod

from pydbclib import connect
from pydbclib.utils import get_columns

from pyetl.es import ES
from pyetl.utils import Singleton


class Writer(ABC):
    default_batch_size = 100000

    @abstractmethod
    def write(self, dataset):
        pass


class DatabaseWriter(Writer):

    def __init__(self, uri, table_name, batch_size=None):
        self.db = connect(uri)
        self.table_name = table_name
        self.batch_size = batch_size or self.default_batch_size

    def write(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset, batch_size=self.batch_size)


class ElasticSearchWriter(Writer):

    def __init__(self, hosts, index_name, doc_type=None, parallel_num=4, batch_size=10000, es_params=None):
        class SingletonES(ES, metaclass=Singleton):
            def __init__(self):
                super().__init__(hosts=hosts, **es_params)
        self.es_singleton_class = SingletonES
        if es_params is None:
            es_params = {}
        self._client = None
        self._index = None
        self.index_name = index_name
        self.doc_type = doc_type
        self.batch_size = batch_size or self.default_batch_size
        self.parallel_num = parallel_num

    @property
    def client(self):
        if self._client is None:
            self._client = self.es_singleton_class()
        return self._client

    @property
    def index(self):
        if self._index is None:
            self._index = self.client.get_index(self.index_name, self.doc_type)
        return self._index

    def write(self, dataset):
        self.index.parallel_bulk(docs=dataset, batch_size=self.batch_size, thread_count=self.parallel_num)


class HiveWriter(Writer):
    """
    insert dataset to hive table by 'insert into' sql
    """

    def __init__(self, uri, table_name, batch_size=None):
        self.db = connect(uri)
        self.table_name = table_name
        self.batch_size = batch_size or self.default_batch_size
        self._columns = None

    @property
    def columns(self):
        if self.columns is None:
            self.db.write(f"select * from {self.table_name} limit 0")
            self._columns = get_columns(self.db.driver.description())
        return self._columns

    def complete_all_fields(self, record):
        return {k: record.get(k, "") for k in self.columns}

    def write(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset.map(self.complete_all_fields), batch_size=self.batch_size)


class HiveWriter2(HiveWriter):
    """
    insert dataset to hive table by 'load data' sql
    """

    def __init__(self, uri, table_name, batch_size=None):
        super().__init__(uri, table_name, batch_size)
        self.local_file_name = self._get_local_file_name()

    def _get_local_file_name(self):
        # 注意 table_name 可能是多表关联多情况，如 t1 left t2 using(uuid)
        code = random.randint(1000, 9999)
        return f"pyetl_dst_table_{'_'.join(self.table_name.split())}_{code}"

    def write(self, dataset):
        self.to_csv(dataset)
        self.load_data()

    def to_csv(self, dataset):
        # dataset.to_df().to_csv(tmp_file, index=None, header=False, sep="\001", columns=self.columns)
        dataset.map(self.complete_all_fields).to_csv(
            self.local_file_name, header=False, sep="\001", columns=self.columns, batch_size=self.batch_size)

    def load_data(self):
        if os.system(f"hadoop fs -put {self.local_file_name} /tmp/{self.local_file_name}") == 0:
            self.db.write(f"load data inpath '/tmp/{self.local_file_name}' into table {self.table_name}")
        else:
            print("上传HDFS失败:", self.local_file_name)


class FileWriter(Writer):

    def __init__(self, file_path, batch_size=None, header=True, sep=","):
        self.file_path = file_path
        self.batch_size = batch_size or self.default_batch_size
        self.header = header
        self.sep = sep

    def write(self, dataset):
        dataset.to_csv(self.file_path, header=self.header, sep=self.sep, batch_size=self.batch_size)
