# -*- coding: utf-8 -*-
"""
@time: 2020/5/6 4:30 下午
@desc:
"""
from pyetl import Task, Reader, Writer, Mapping

r = Reader("mysql+pymysql://root:lyt124878@localhost:3306/test", "src")
w = Writer("mysql+pymysql://root:lyt124878@localhost:3306/test", "dst")
m1 = Mapping(columns={"uuid": "a", "name": "b"}, functions={"uuid": lambda x: x + 20})
task = Task(r, m1, w)
task.start()
