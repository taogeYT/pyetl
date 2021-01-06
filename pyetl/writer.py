# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
import hashlib
import os
import random
import shutil
import sys
import time
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
            r = self.db.execute(f"select * from {self.table_name} limit 0")
            r.fetchall()
            self._columns = r.get_columns()
        return self._columns

    def complete_all_fields(self, record):
        return {k: record.get(k, "") for k in self.columns}

    def write(self, dataset):
        self.db.get_table(self.table_name).bulk(dataset.map(self.complete_all_fields), batch_size=self.batch_size)


class HiveWriter2(HiveWriter):
    """
    insert dataset to hive table by 'load data' sql
    """
    cache_file = ".pyetl_hive_cache"

    def __init__(self, db, table_name, batch_size=1000000, hadoop_path=None, delimited="\001"):
        super().__init__(db, table_name, batch_size)
        self.file_name = self._get_local_file_name()
        self.local_path = os.path.join(self.cache_file, self.file_name)
        self.delimited = delimited
        self.hadoop = hadoop_path if hadoop_path else "hadoop"

    def _get_local_file_name(self):
        # 注意 table_name 可能是多表关联多情况，如 t1 left t2 using(uuid)
        # code = random.randint(1000, 9999)
        # return f"pyetl_dst_table_{'_'.join(self.table_name.split())}_{code}"
        uuid = hashlib.md5(self.table_name.encode())
        return f"{uuid.hexdigest()}-{int(time.time())}"

    def clear(self):
        shutil.rmtree(self.local_path)

    def write(self, dataset):
        file_writer = FileWriter(
            self.local_path, header=False, sep=self.delimited, columns=self.columns, batch_size=self.batch_size)
        file_writer.write(dataset.map(self.complete_all_fields))
        try:
            self.load_data()
        finally:
            self.clear()

    def to_csv(self, dataset):
        dataset.map(self.complete_all_fields).to_csv(
            self.local_path, header=False, sep="\001", columns=self.columns, batch_size=self.batch_size)

    def load_data(self):
        if os.system(f"{self.hadoop} fs -put {self.local_path} /tmp/{self.file_name}") == 0:
            try:
                self.db.execute(f"load data inpath '/tmp/{self.file_name}' into table {self.table_name}")
            finally:
                os.system(f"{self.hadoop} fs -rm -r /tmp/{self.file_name}")
        else:
            print("上传HDFS失败:", self.file_name)


class FileWriter(Writer):

    def __init__(self, file_path, file_name=None, batch_size=None, header=True, sep=",", columns=None):
        self.file_path = file_path
        self.file_name = file_name
        if not os.path.exists(file_path):
            os.makedirs(self.file_path)
        self.batch_size = batch_size or self.default_batch_size
        self.kw = dict(header=header, sep=sep, columns=columns)

    def write(self, dataset):
        if self.file_name:
            dataset.to_csv(os.path.join(self.file_path, self.file_name), batch_size=self.batch_size, **self.kw)
        else:
            self.to_csv_files(dataset, self.file_path, batch_size=self.batch_size, **self.kw)

    @classmethod
    def to_csv_files(cls, dataset, path, batch_size=100000, **kwargs):
        for i, df in enumerate(dataset.to_df(batch_size=batch_size)):
            file = os.path.join(path, f"{i:0>8}")
            df.to_csv(file, index=False, **kwargs)
