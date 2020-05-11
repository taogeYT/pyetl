# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
import os
import random
from abc import ABC, abstractmethod

from pydbclib import connect

from pyetl.es import ES


class BaseWriter(ABC):

    @abstractmethod
    def write(self, dataset):
        pass

    def before(self):
        pass

    def after(self):
        pass


class Writer(BaseWriter):

    def __init__(self, uri, table_name):
        self.db = connect(uri)
        self.table_name = table_name

    def write(self, dataset):
        self.before()
        if self.exists_table():
            self.db.get_table(self.table_name).bulk_insert(dataset)
            self.after()
        else:
            print(f"no such table: {self.table_name}")

    def exists_table(self):
        try:
            self.db.read_one(f"select 1 from {self.table_name}")
            return True
        except Exception:
            return False


class ESWriter(BaseWriter):

    def __init__(self, hosts, index_name, doc_type=None):
        self.es = ES(hosts=hosts, timeout=60)
        self.index_name = index_name
        self.doc_type = doc_type

    def write(self, dataset):
        self.es.get_index(self.index_name, self.doc_type).bulk_insert(dataset)


class HiveWriter(BaseWriter):

    def __init__(self, uri, table_name):
        self.db = connect(uri)
        self.table_name = table_name
        self.columns = self.db.get_table(self.table_name).get_columns()
        code = random.randint(1000, 9999)
        self.local_file_name = f"pyetl_dst_table_{self.table_name}_{code}"

    def complete_fields(self, record):
        return {k: record.get(k, "") for k in self.columns}

    def write(self, dataset):
        # dataset.to_df().to_csv(tmp_file, index=None, header=False, sep="\001", columns=self.columns)
        dataset.map(self.complete_fields).to_csv(self.local_file_name, header=False, sep=",", columns=self.columns)
        self.load_data()

    def load_data(self):
        if os.system(f"hadoop fs -put {self.local_file_name} /tmp/{self.local_file_name}") == 0:
            self.db.execute(f"load data inpath '/tmp/{self.local_file_name}' into table {self.table_name}")
        else:
            print("上传HDFS失败:", self.local_file_name)
