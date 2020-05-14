# -*- coding: utf-8 -*-
"""
@time: 2020/4/30 11:29 上午
@desc:
"""
import time
import functools


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


def print_run_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        r = func(*args, **kwargs)
        cost = time.time() - start
        cost = round(cost, 3)
        print(f"{func.__name__}函数执行了{cost}s")
        return r
    return wrapper


@print_run_time
def main():
    pass


if __name__ == '__main__':
    main()
