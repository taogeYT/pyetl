# -*- coding: utf-8 -*-
"""
@time: 2020/4/29 10:58 上午
@desc:
"""


class Task(object):

    def __init__(self, reader, mapping, writer):
        self.reader = reader
        self.writer = writer
        self.mapping = mapping
        self.mapping.register(self.reader, self.writer)

    def start(self):
        self.load(self.transform(self.extract()))

    def extract(self):
        return self.reader.read(self.mapping)

    def transform(self, dataset):
        return self.mapping.transform(dataset)

    def load(self, dataset):
        self.writer.write(dataset)
