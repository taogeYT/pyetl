# -*- coding: utf-8 -*-
"""
@time: 2020/5/25 11:47 下午
@desc:
"""
import itertools

import pandas


class Dataset(object):

    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return self

    def next(self):
        return next(self._rows)

    __next__ = next

    def map(self, function):
        self._rows = (function(r) for r in self._rows)
        return self

    def filter(self, function):
        self._rows = (r for r in self._rows if function(r))
        return self

    def rename(self, mapper):
        """
        字段重命名
        """
        def function(record):
            if isinstance(record, dict):
                return {mapper.get(k, k): v for k, v in record.items()}
            else:
                return dict(zip(mapper, record))
        return self.map(function)

    def limit(self, num):
        self._rows = (r for i, r in enumerate(self._rows) if i < num)
        return self

    def get_one(self):
        r = self.get(1)
        return r[0] if len(r) > 0 else None

    def get(self, num):
        return [i for i in itertools.islice(self._rows, num)]

    def get_all(self):
        return [r for r in self._rows]

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
