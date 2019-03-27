from sqlalchemy import create_engine
from sqlalchemy.types import String
from sqlalchemy import engine
from collections import Iterator
import pandas
import functools
import time
import logging
from pyetl.db import connection
from pyetl.mylogger import log


def print(*args, notice=''):
    log.debug('%s\n%s' % (notice, ' '.join(['%s' % i for i in args])))


def run_time(func):
    """
    记录函数执行时间
    """
    @functools.wraps(func)
    def wrapper(*args, **kw):
        T = time.time()
        rs = func(*args, **kw)
        log.info('function [%s] run: %ss' % (
            func.__name__, round(time.time() - T, 3)))
        return rs
    return wrapper


def to_lower(x):
    """
    输入中的元素转小写
    >>> to_lower('ABC')
    'abc'
    >>> to_lower(['ABc','CDE'])
    ['abc', 'cde']
    """
    if isinstance(x, list):
        return [i.lower() for i in x]
    else:
        return x.lower()


class EtlUtil():
    __task_table = 'py_script_task'
    __query_size = 2000000
    __insert_size = 200000
    __field_default_size = 200
    __print_debug = False

    @classmethod
    def config(cls, cfg):
        # for i in dir(cfg):
        #     if not i.startswith('_'):
        #         getattr(cls, i)
        cls.__task_table = getattr(cfg, 'TASK_TABLE', cls.__task_table)
        cls.__query_size = getattr(cfg, 'QUERY_SIZE', cls.__query_size)
        cls.__insert_size = getattr(cfg, 'INSERT_SIZE', cls.__insert_size)
        cls.__field_default_size = getattr(
            cfg, 'NEW_TABLE_FIELD_DEFAULT_SIZE', cls.__field_default_size)
        cls.src_db_uri = getattr(cfg, 'SRC_DB_URI', None)
        cls.dst_db_uri = getattr(cfg, 'DST_DB_URI', None)
        cls.__print_debug = getattr(cfg, 'DEBUG', cls.__print_debug)
        if cls.__print_debug:
            log.setLevel(logging.DEBUG)

    def __init__(self, src_table, dst_table, field_map, update_field=None,
                 unique_field=None, src_db_uri=None, dst_db_uri=None):
        self.src_table = src_table.upper()
        self.dst_table = dst_table.upper()
        if src_db_uri is None:
            src_db_uri = EtlUtil.src_db_uri
        if dst_db_uri is None:
            dst_db_uri = EtlUtil.dst_db_uri
        if '://' in src_db_uri:
            self.src_engine = create_engine(src_db_uri,
                                            echo=EtlUtil.__print_debug)
        else:
            self.src_engine = src_db_uri
        if '://' in dst_db_uri:
            self.dst_engine = create_engine(dst_db_uri,
                                            echo=EtlUtil.__print_debug)
        else:
            self.dst_engine = dst_db_uri
        self.field_map = {
            to_lower(i): to_lower(j) for i, j in field_map.items()
        }
        self.funs = {i: lambda x: x for i in self.field_map}
        if unique_field and update_field:
            self.unique_field = self.field_map.get(
                to_lower(unique_field), None)
            self.dst_unique = to_lower(unique_field)
        else:
            self.unique_field = None
        if update_field:
            self.update_field = to_lower(update_field)
            sql = ("select last_time from {task_table} "
                   "where src_table='{src_table}'"
                   "and dst_table='{dst_table}'".format(
                       task_table=self.__task_table,
                       src_table=self.src_table,
                       dst_table=self.dst_table))
            self._cron = self.query(sql)
            if self._cron:
                self.last_time = self._cron[0][0]
            else:
                self.last_time = None
        else:
            self.update_field = None

    def add(self, col):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kw):
                return func(*args, **kw)
            self.funs[col.lower()] = func
            return wrapper
        return decorator

    @classmethod
    def query(cls, sql, engine=None):
        """
        sql查询操作
        """
        if engine is None:
            engine = cls.dst_db_uri
        with connection(engine) as db:
            log.info(engine)
            res = db.query(sql)
        return res

    def _query_data_generator(self, sql, args):
        with connection(self.src_engine) as db:
            res = db.query_dict(sql, args, chunksize=self.__query_size)
            for r in res:
                data = {}
                for i, d in r:
                    data[i] = pandas.Series(d)
                rs = pandas.DataFrame(data)
                yield rs

    def query_data(self, sql, args):
        if isinstance(self.src_engine, engine.base.Engine):
            iter_df = pandas.io.sql.read_sql_query(
                sql, self.src_engine,
                params=args,
                chunksize=self.__query_size)
            return iter_df
        else:
            return self._query_data_generator(sql, args)

    def handle_sql(self, where, groupby):
        self.src_field = []
        for i in self.field_map.values():
            if isinstance(i, list):
                self.src_field.extend(i)
            else:
                self.src_field.append(i)
        if self.update_field and self.update_field not in self.src_field:
                self.src_field.append(self.update_field)
        sql = ["select {columns} from {src_table}".format(
            columns=','.join(self.src_field), src_table=self.src_table)]
        if self.last_time:
            sql.append("where %s>:1" % (
                self.update_field))
            args = [self.last_time]
            if where:
                sql.append('and (%s)' % where)
        else:
            args = None
            if where:
                sql.append('where (%s)' % where)
        if groupby:
            sql.append('group by')
            sql.append(groupby)
        sql = ' '.join(sql)
        return sql, args

    def generate_dataframe(self, sql, args):
        """
        获取源数据
        """
        iter_df = self.query_data(sql, args)
        for src_df in iter_df:
            if isinstance(self.src_engine, engine.base.Engine):
                src_df.rename(
                    columns={
                        i: j for i, j in zip(src_df.columns, self.src_field)
                    },
                    inplace=True
                )
            # if EtlUtil.__print_debug:
            #     print('src table info')
            #     src_df.info()
            print(src_df[:4])
            if self.update_field:
                last_time = src_df[self.update_field.lower()].max()
                print(last_time, type(last_time))
                print(self.last_time, type(self.last_time))
                self.last_time = max(
                    self.last_time, last_time) if self.last_time else last_time
            yield src_df

    def run(self, where=None, groupby=None):
        iter_df = self.generate_dataframe(*self.handle_sql(where, groupby))
        for src_df in iter_df:
            if self.unique_field:
                src_df = src_df.sort_index(
                    by=self.update_field, ascending=False)
                # print(src_df.duplicated())
                src_df = src_df.drop_duplicates([self.unique_field])
            data = {}
            for i, j in self.field_map.items():
                if isinstance(j, list):
                    data[i] = pandas.Series(
                        map(list, zip(*[src_df[x] for x in j]))
                    ).map(self.funs[i])
                else:
                    data[i] = src_df[j].map(self.funs[i])
            dst_df = pandas.DataFrame(data)
            # if EtlUtil.__print_debug:
            #     print('dst table info')
            #     dst_df.info()
            print(dst_df[:4])
            yield dst_df

    @run_time
    def join(self, *args, on=''):
        """
        合并数据入库
        """
        flag = None
        if len(args) == 1:
            for df in args[0]:
                flag = True
                self.to_save(df)
            else:
                log.info('没有数据更新') if flag is None else None
        else:
            args = map(next, args) if isinstance(args[0], Iterator) else args
            if len(args) == 2:
                rs = pandas.merge(*args, on=on, how='outer')
                self.to_save(rs)
            else:
                rs = pandas.merge(args[0], args[1], on=on, how='outer')
                new_args = (rs,) + args[2:]
                self.join(*new_args, on=on)

    def to_save(self, df):
        """
        保存数据
        记录最后更新的时间点
        """
        if self.unique_field and self._cron:
            # df.to_dict(orient='dict')
            columns = list(df.columns)
            df = [dict(zip(columns, i)) for i in df.values]
            with connection(self.dst_engine) as db:
                db.merge(self.dst_table, df, columns, self.dst_unique)
        else:
            df.to_sql(self.dst_table.lower(), self.dst_engine,
                      dtype=String(self.__field_default_size),
                      if_exists='append',
                      chunksize=self.__insert_size, index=False)
        log.info('插入数量：%s' % len(df))
        if self._cron:
            self.dst_engine.execute(
                "update %s set last_time=:1 "
                "where src_table=:1 "
                "and dst_table=:1" % self.__task_table,
                self.last_time,
                self.src_table,
                self.dst_table)
        else:
            self.dst_engine.execute(
                "insert into %s(last_time,src_table,dst_table) "
                "values(:1,:1,:1)" % self.__task_table,
                self.last_time,
                self.src_table,
                self.dst_table)
