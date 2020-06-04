# -*- coding: utf-8 -*-
"""
@time: 2020/5/26 11:40 上午
@desc:
"""
from pyetl.mapping import ColumnsMapping, Mapping
from pyetl.utils import print_run_time, validate_param


class Task(object):
    _dataset = None
    reader = None
    writer = None
    columns = None
    functions = None

    def __init__(self, reader=None, writer=None, columns=None, functions=None):
        if reader is not None:
            self.reader = reader
        if writer is not None:
            self.writer = writer
        if not getattr(self, 'reader', None):
            raise ValueError("%s must have a reader" % type(self).__name__)
        if columns is not None:
            self.columns = columns
        if functions is not None:
            self.functions = validate_param("functions", functions, dict)
        self.columns = self.get_columns()
        self.functions = self.get_functions()
        self.columns_mapping = ColumnsMapping(self.columns)
        self.mapping = Mapping(self.columns_mapping.columns, self.functions, self.apply_function)

    def get_columns(self):
        if self.columns is None:
            return {col: col for col in self.reader.columns}
        if isinstance(self.columns, dict):
            return {i: j for i, j in self.columns.items()}
        elif isinstance(self.columns, set):
            return {c: c for c in self.columns}
        else:
            raise ValueError("columns 参数错误")

    def get_functions(self):
        if self.functions:
            return self.functions
        else:
            return {}

    def apply_function(self, record):
        return record

    def filter_function(self, record):
        return True

    def before(self):
        pass

    def after(self):
        pass

    def show(self, num=10):
        self.dataset.show(num)

    @property
    def total(self):
        return self.mapping.total

    @property
    def dataset(self):
        if self._dataset is None:
            self._dataset = self.reader.read(self.columns_mapping.alias).map(self.mapping).filter(self.filter_function)
        return self._dataset

    @print_run_time
    def start(self):
        if not getattr(self, "writer", None):
            raise ValueError("%s must have a writer" % type(self).__name__)
        self.before()
        self.dataset.write(self.writer)
        self.after()

