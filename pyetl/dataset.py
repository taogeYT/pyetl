# -*- coding: utf-8 -*-
"""
@time: 2020/5/25 11:47 下午
@desc:
"""
import itertools

import pandas

from pyetl.utils import limit_iterator


class Dataset(object):

    def __init__(self, rows):
        self._rows = rows
        self.total = 0

    def __iter__(self):
        return self

    def next(self):
        self.total += 1
        return next(self._rows)

    __next__ = next

    def map(self, function):
        self._rows = (function(r) for r in self._rows)
        return self

    def filter(self, function):
        self._rows = (r for r in self._rows if function(r))
        return self

    def rename(self, columns):
        """
        字段重命名
        """
        def function(record):
            if isinstance(record, dict):
                return {columns.get(k, k): v for k, v in record.items()}
            else:
                raise ValueError("only rename dict record")
        return self.map(function)

    def rename_and_extract(self, columns):
        """
        字段投影，字段不存在的默认等于None
        """
        def function(record):
            if isinstance(record, dict):
                return {v: record.get(k) for k, v in columns.items()}
            else:
                raise ValueError("only rename dict record")
        return self.map(function)

    def limit(self, num):
        self._rows = limit_iterator(self._rows, num)
        return self

    def get_one(self):
        r = self.get(1)
        return r[0] if len(r) > 0 else None

    def get(self, num):
        return [i for i in itertools.islice(self._rows, num)]

    def get_all(self):
        return [r for r in self._rows]

    def to_batch(self, size=10000):
        while 1:
            batch = self.get(size)
            if batch:
                yield batch
            else:
                return None

    def show(self, num=10):
        for data in self.limit(num):
            print(data)

    def write(self, writer):
        writer.write(self)

    def to_df(self, batch_size=None):
        if batch_size is None:
            return pandas.DataFrame.from_records(self)
        else:
            return self._to_df_iterator(batch_size)

    def _to_df_iterator(self, batch_size):
        while 1:
            records = self.get(batch_size)
            if records:
                yield pandas.DataFrame.from_records(records)
            else:
                return None

    def to_csv(self, file_path, sep=',', header=False, columns=None, batch_size=100000):
        """
        用于大数据量分批写入文件
        :param file_path: 文件路径
        :param sep: 分割符号，hive默认\001
        :param header: 是否写入表头
        :param columns: 按给定字段排序
        :param batch_size: 每批次写入文件行数
        """
        mode = "w"
        for df in self.to_df(batch_size=batch_size):
            df.to_csv(file_path, sep=sep, index=False, header=header, columns=columns, mode=mode)
            mode = "a"
            header = False
