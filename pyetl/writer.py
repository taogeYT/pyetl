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
BATCH_SIZE = 100000


class BaseWriter(ABC):

    def write(self, dataset):
        self.before()
        self.execute(dataset)
        self.after()

    def before(self):
        pass

    def after(self):
        pass

    @abstractmethod
    def execute(self, dataset):
        pass


class Writer(BaseWriter):

    def __init__(self, uri, table_name, batch_size=BATCH_SIZE):
        self.db = connect(uri)
        self.table_name = table_name
        self.batch_size = batch_size

    def execute(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset, batch_size=self.batch_size)


class ElasticSearchWriter(BaseWriter):

    def __init__(self, hosts, index_name, doc_type=None, parallel_num=1, batch_size=10000, es_params=None):
        if es_params is None:
            es_params = {}
        self.es = ES(hosts=hosts, **es_params)
        self.index_name = index_name
        self.doc_type = doc_type
        self.batch_size = batch_size
        self.parallel_num = parallel_num
        self.index = self.es.get_index(index_name, doc_type)

    def execute(self, dataset):
        if self.parallel_num > 1:
            self.index.parallel_bulk(docs=dataset, batch_size=self.batch_size, thread_count=self.parallel_num)
        else:
            self.index.bulk(dataset, batch_size=self.batch_size)


class HiveWriter(BaseWriter):
    """
    insert dataset to hive table by 'insert into' sql
    """

    def __init__(self, uri, table_name, batch_size=BATCH_SIZE):
        self.db = connect(uri)
        self.table_name = table_name
        self.batch_size = batch_size
        self.columns = self._get_table_columns()

    def _get_table_columns(self):
        self.db.execute(f"select * from {self.table_name} limit 0")
        return get_columns(self.db.driver.description())

    def complete_all_fields(self, record):
        return {k: record.get(k, "") for k in self.columns}

    def execute(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset.map(self.complete_all_fields), batch_size=self.batch_size)


class HiveWriter2(HiveWriter):
    """
    insert dataset to hive table by 'load data' sql
    """

    def __init__(self, uri, table_name, batch_size=BATCH_SIZE):
        super().__init__(uri, table_name, batch_size)
        self.local_file_name = self._get_local_file_name()

    def _get_local_file_name(self):
        # 注意 table_name 可能是多表关联多情况，如 t1 left t2 using(uuid)
        code = random.randint(1000, 9999)
        return f"pyetl_dst_table_{'_'.join(self.table_name.split())}_{code}"

    def execute(self, dataset):
        self.to_csv(dataset)
        self.load_data()

    def to_csv(self, dataset):
        # dataset.to_df().to_csv(tmp_file, index=None, header=False, sep="\001", columns=self.columns)
        dataset.map(self.complete_all_fields).to_csv(
            self.local_file_name, header=False, sep=",", columns=self.columns, batch_size=self.batch_size)

    def load_data(self):
        if os.system(f"hadoop fs -put {self.local_file_name} /tmp/{self.local_file_name}") == 0:
            self.db.execute(f"load data inpath '/tmp/{self.local_file_name}' into table {self.table_name}")
        else:
            print("上传HDFS失败:", self.local_file_name)


class FileWriter(BaseWriter):

    def __init__(self, file_name, batch_size=BATCH_SIZE, header=True, sep=","):
        self.file_name = file_name
        self.batch_size = batch_size
        self.header = header
        self.sep = sep

    def execute(self, dataset):
        dataset.to_csv(self.file_name, header=self.header, sep=self.sep, batch_size=self.batch_size)
