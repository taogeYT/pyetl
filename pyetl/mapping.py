# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:29 上午
@desc:
"""


class ColumnsMapping(object):

    def __init__(self, columns):
        self.raw_columns = columns
        self.alias, self.columns = self.get_src_columns_alias()

    def get_src_columns_alias(self):
        alias = {}
        for k, v in self.raw_columns.items():
            if isinstance(v, str):
                alias.setdefault(v, k)
            else:
                for i, name in enumerate(v):
                    alias.setdefault(name, "%s_%s" % (k, i))

        columns = {}
        for k, v in self.raw_columns.items():
            if isinstance(v, str):
                columns[k] = alias[v]
            else:
                columns[k] = (alias[n] for n in v)
        return alias, columns


class Mapping(object):

    def __init__(self, columns, functions):
        self.columns = columns
        self.functions = functions

    def __call__(self, record):
        result = {}
        for k, v in self.columns.items():
            if isinstance(v, str):
                value = record[v]
                result[k] = self.functions[k](value) if k in self.functions else value
            else:
                result[k] = self.functions.get(k, lambda x: ",".join(map(str, x)))(tuple(record[n] for n in v))
        return result
