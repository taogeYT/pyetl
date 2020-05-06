# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
from abc import ABC, abstractmethod

from pydbclib import connect


class BaseReader(ABC):

    @abstractmethod
    def read(self, mapping):
        pass


class Reader(BaseReader):

    def __init__(self, uri, table_name, condition=None):
        self.db = connect(uri)
        self.table_name = table_name
        self.condition = condition if condition else "1=1"

    def get_query_text(self, mapping):
        columns = [f"{name} as {alias}" for name, alias in zip(mapping.src_raw_fields, mapping.src_alias_fields)]
        texts = ["select", ','.join(columns), "from", self.table_name]
        return " ".join(texts)

    def read(self, mapping):
        text = self.get_query_text(mapping)
        if isinstance(self.condition, str):
            text = f"{text} where {self.condition}"
            return self.db.read(text)
        elif callable(self.condition):
            return self.db.read(text).filter(self.condition)
        else:
            raise ValueError("condition 参数错误")
