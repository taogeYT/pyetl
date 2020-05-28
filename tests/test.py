# -*- coding: utf-8 -*-
"""
@time: 2020/5/6 4:30 下午
@desc:
"""
import os
import unittest

from pydbclib import connect

from pyetl.dataset import Dataset
from pyetl.jobs import Task
from pyetl.reader import DatabaseReader, FileReader, ExcelReader
from pyetl.writer import DatabaseWriter, FileWriter


class BaseTest(unittest.TestCase):
    db = None
    src_record = {"uuid": 1, "full_name": "python ETL framework"}
    dst_record = {"id": 1, "name": "python ETL framework"}
    columns = {"uuid": "id", "full_name": "name"}

    @classmethod
    def get_file_path(cls, name):
        return os.path.join(os.path.dirname(__file__), 'data', name)

    @classmethod
    def setUpClass(cls):
        path = cls.get_file_path("src.db")
        path = "sqlite:///" + path
        cls.db = connect(path)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.close()


class TestReader(BaseTest):

    def validate(self, reader):
        self.assertEqual(reader.columns, ["uuid", "full_name"])
        r = reader.read(columns=self.columns)
        self.assertTrue(isinstance(r, Dataset))
        self.assertEqual(r.get_all(), [self.dst_record])

    def test_db_reader(self):
        reader = DatabaseReader(self.db, "src")
        self.validate(reader)

    def test_file_reader(self):
        reader = FileReader(self.get_file_path("src.txt"))
        self.validate(reader)

    def test_excel_reader(self):
        reader = ExcelReader(self.get_file_path("src.xlsx"))
        self.validate(reader)


class TestWriter(BaseTest):

    def test_db_reader(self):
        writer = DatabaseWriter(self.db, "dst")
        writer.table.delete("1=1")
        ds = Dataset(iter([self.dst_record]))
        ds.write(writer)
        r = Task(DatabaseReader(self.db, "dst")).read_and_mapping()
        self.assertEqual(r.get_all(), [self.dst_record])
