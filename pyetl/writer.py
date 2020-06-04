# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
import os
import random
import sys
from abc import ABC, abstractmethod
from multiprocessing.pool import Pool

from pyetl.connections import DatabaseConnection, ElasticsearchConnection
from pyetl.es import bulk_insert
from pyetl.utils import batch_dataset


class Writer(ABC):
    default_batch_size = 100000

    @abstractmethod
    def write(self, dataset):
        pass


class DatabaseWriter(DatabaseConnection, Writer):

    def __init__(self, db, table_name, batch_size=None):
        super().__init__(db)
        self.table_name = table_name
        self.table = self.db.get_table(self.table_name)
        self.batch_size = batch_size or self.default_batch_size

    def write(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset, batch_size=self.batch_size)


class ElasticsearchWriter(ElasticsearchConnection, Writer):

    def __init__(self, index_name, doc_type=None, es_params=None, parallel_num=None, batch_size=10000):
        super().__init__(es_params)
        self._index = None
        self.index_name = index_name
        self.doc_type = doc_type
        self.batch_size = batch_size or self.default_batch_size
        self.parallel_num = parallel_num
        self.index = self.client.get_index(self.index_name, self.doc_type)

    def write(self, dataset):
        if self.parallel_num is None or "win" in sys.platform:
            self.index.parallel_bulk(docs=dataset, batch_size=self.batch_size)
        else:
            pool = Pool(self.parallel_num)
            for batch in batch_dataset(dataset, self.batch_size):
                pool.apply_async(bulk_insert, args=(self.es_params, batch, self.index.name, self.index.doc_type))
            pool.close()
            pool.join()


class HiveWriter(DatabaseConnection, Writer):
    """
    insert dataset to hive table by 'insert into' sql
    """

    def __init__(self, db, table_name, batch_size=None):
        super().__init__(db)
        self.table_name = table_name
        self.batch_size = batch_size or self.default_batch_size
        self._columns = None

    @property
    def columns(self):
        if self._columns is None:
            self.db.execute(f"select * from {self.table_name} limit 0")
            self._columns = self.db.get_columns()
        return self._columns

    def complete_all_fields(self, record):
        return {k: record.get(k, "") for k in self.columns}

    def write(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset.map(self.complete_all_fields), batch_size=self.batch_size)


class HiveWriter2(HiveWriter):
    """
    insert dataset to hive table by 'load data' sql
    """

    def __init__(self, db, table_name, batch_size=None):
        super().__init__(db, table_name, batch_size)
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
