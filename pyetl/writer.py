# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:28 上午
@desc:
"""
from abc import ABC, abstractmethod

from pydbclib import connect


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
