# from py_db import connection
from pydbclib import connection
from dateutil.parser import parse
import datetime
from pyetl.utils import dtutil
from pyetl.logger import log
import re


class TaskConfig(object):

    def __init__(self, src_table, dst_table, debug=False, task_table="task"):
        # instance_log(self, debug)
        self.db = connection('task.db', driver='sqlite3', debug=debug)
        m = re.compile('\s+')
        self.task_table = task_table
        self.src_table = m.sub('', src_table.lower())
        self.dst_table = dst_table.lower()
        self.init_db()

    def exist_table(self):
        sql = ("SELECT COUNT(*) FROM sqlite_master"
               " where type='table' and name='%s'") % self.task_table
        return self.db.query(sql)[0][0]

    def init_db(self):
        sql = ("create table if not exists"
               " %s(id varchar(500) primary key,"
               " src_table varchar(1000),"
               " dst_table varchar(100),"
               " date_type varchar(1),"
               " last_time varchar(20))") % self.task_table
        self.db.insert(sql)

    def append(self, last_time=None):
        id = self.src_table + "||" + self.dst_table
        exist = self.db.query(
            "select * from {} where id=:1".format(self.task_table), [id])
        if exist:
            return None
        if isinstance(last_time, datetime.datetime):
            date_type = 1
            last_time = str(last_time)
        else:
            date_type = 0
        self.db.insert(
            "insert into %s(id,last_time,date_type,src_table,dst_table) "
            "values(:1,:1,:1,:1,:1)" % self.task_table,
            [id, last_time, date_type, self.src_table, self.dst_table]
        )
        # self.db.commit()

    def update(self, last_time):
        if isinstance(last_time, datetime.datetime):
            date_type = 1
            last_time = str(last_time)
        else:
            date_type = 0
        self.db.insert(
            "update %s set last_time=:1,date_type=:1 "
            "where src_table=:1 and dst_table=:1" % self.task_table,
            [last_time, date_type, self.src_table, self.dst_table]
        )
        # self.db.commit()

    def query(self, days=None):
        if days == 0:
            log.warn("days 参数为 0 全量重跑")
            self.db.insert(
                "delete from {task_table} "
                "where src_table=:1 "
                "and dst_table=:1".format(task_table=self.task_table),
                [self.src_table, self.dst_table])
            return (None, None)
        res = self.db.query(
            "select 1, last_time, date_type from {task_table} "
            "where src_table=:1 "
            "and dst_table=:1".format(task_table=self.task_table),
            [self.src_table, self.dst_table])
        if res:
            if res[0][1]:
                if res[0][2]:
                    dt = parse(dtutil.days_ago_str(date=res[0][1], delta=days))
                else:
                    dt = dtutil.days_ago_str(date=res[0][1], delta=days)
            else:
                dt = res[0][1]
            return res[0][0], dt
        else:
            return (None, None)

    def commit(self):
        self.db.commit()

if __name__ == "__main__":
    job = TaskConfig("tab1", "tab2")
    # job.append(datetime.datetime(2017, 9, 12).__str__())
    # job.modify(datetime.datetime(2017, 10, 19, 14, 34).__str__())
    rs = job.append()
    print(rs)
