# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:29 上午
@desc:
"""


def lower_columns(x):
    if isinstance(x, (list, tuple)):
        return tuple([i.lower() for i in x])
    else:
        return x.lower()


def batch_dataset(dataset, batch_size):
    cache = []
    for data in dataset:
        cache.append(data)
        if len(cache) >= batch_size:
            yield cache
            cache = []
    if cache:
        yield cache
