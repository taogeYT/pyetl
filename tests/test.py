# -*- coding: utf-8 -*-
"""
@time: 2020/5/6 4:30 下午
@desc:
"""
import os
import unittest

from pydbclib import connect

from pyetl.dataset import Dataset
from pyetl.task import Task
from pyetl.reader import DatabaseReader, FileReader, ExcelReader
from pyetl.writer import DatabaseWriter, FileWriter


class BaseTest(unittest.TestCase):
    db = None
    src_record = {"uuid": 1, "full_name": "python ETL framework"}
    dst_record = {"id": 1, "name": "python ETL framework"}
    columns = {"id": "uuid", "name": "full_name"}

    @classmethod
    def get_file_path(cls, name):
        return os.path.join(os.path.dirname(__file__), 'data', name)

    @classmethod
    def setUpClass(cls):
        # path = cls.get_file_path("src.db")
        # path = "sqlite:///" + path
        cls.db = connect("sqlite:///:memory:")
        create_src = """CREATE TABLE "src" ("uuid" INTEGER NOT NULL,"full_name" TEXT,PRIMARY KEY ("uuid"))"""
        cls.db.execute(create_src)
        create_dst = """CREATE TABLE "dst" ("id" INTEGER NOT NULL,"name" TEXT,PRIMARY KEY ("id"))"""
        cls.db.execute(create_dst)
        cls.db.get_table("src").insert(cls.src_record)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.db.execute("drop table src")
        cls.db.execute("drop table dst")
        cls.db.commit()

    @classmethod
    def get_dataset(cls, record):
        return Dataset(iter([record]))


class TestReader(BaseTest):

    def validate(self, reader):
        self.assertEqual(reader.columns, ["uuid", "full_name"])
        r = reader.read(columns={"uuid": "id", "full_name": "name"})
        self.assertTrue(isinstance(r, Dataset))
        self.assertEqual(r.get_all(), self.get_dataset(self.dst_record).get_all())

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

    def test_db_writer(self):
        writer = DatabaseWriter(self.db, "dst")
        writer.table.delete("1=1")
        writer.write(self.get_dataset(self.dst_record))
        task = Task(DatabaseReader(self.db, "dst"))
        self.assertEqual(task.dataset.get_all(), self.get_dataset(self.dst_record).get_all())

    def test_file_writer(self):
        file = self.get_file_path("dst.txt")
        path, name = os.path.split(file)
        writer = FileWriter(path, name)
        writer.write(self.get_dataset(self.dst_record))
        task = Task(FileReader(file))
        self.assertEqual(task.dataset.get_all(), self.get_dataset(self.dst_record).get_all())


class TestTask(BaseTest):

    def test_no_columns(self):
        reader = DatabaseReader(self.db, "src")
        task = Task(reader)
        self.assertEqual(task.dataset.get_all(), [self.src_record])

    def test_set_columns(self):
        reader = DatabaseReader(self.db, "src")
        task = Task(reader, columns={"uuid"})
        self.assertEqual(task.dataset.get_all(), [{"uuid": 1}])

    def test_dict_columns(self):
        reader = DatabaseReader(self.db, "src")
        task = Task(reader, columns=self.columns)
        self.assertEqual(task.dataset.get_all(), self.get_dataset(self.dst_record).get_all())


if __name__ == '__main__':
    unittest.main()
