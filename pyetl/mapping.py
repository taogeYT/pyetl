# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:29 上午
@desc:
"""
from pyetl.utils import lower_columns


class Mapping(object):
    columns = None
    functions = None

    def __init__(self, columns=None, functions=None, apply_function=None):
        self.reader = None
        self.writer = None
        self.src_raw_fields = None
        self.src_alias_fields = None
        self.alias_columns = None
        self.functions = functions or self.functions
        self.functions = self.get_functions()
        self.columns = columns or self.columns
        self.columns = self.get_columns()
        self.apply_function = apply_function
        if isinstance(self.columns, dict):
            self.src_raw_fields, self.src_alias_fields, self.alias_columns = self.init_columns()

    def get_functions(self):
        return self.functions if self.functions else {}

    def get_columns(self):
        if isinstance(self.columns, dict):
            return {lower_columns(i): j for i, j in self.columns.items()}
        elif isinstance(self.columns, set):
            return {c: c for c in self.columns}
        else:
            raise ValueError("columns 参数错误")

    def register(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def init_columns(self):
        src_raw_fields = []
        src_alias_fields = []
        alias_columns = {}
        for k, v in self.columns.items():
            if isinstance(v, (list, tuple)):
                src_raw_fields.extend(v)
                alias = [f"{k}{i}" for i, n in enumerate(v)]
                src_alias_fields.extend(alias)
                alias_columns[k] = alias
            else:
                src_raw_fields.append(v)
                src_alias_fields.append(k)
                alias_columns[k] = k
        return src_raw_fields, src_alias_fields, alias_columns

    def transform_function(self, record):
        rs = {}
        for k, v in self.alias_columns.items():
            if isinstance(v, str):
                rs[k] = self.functions[k](record[v]) if k in self.functions else record[v]
            else:
                rs[k] = self.functions.get(k, lambda x: ",".join(map(str, x)))([record[i] for i in v])
        return self.apply_function(rs) if self.apply_function else rs

    def transform(self, dataset):
        return dataset.map(self.transform_function)
