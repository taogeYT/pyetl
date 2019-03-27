from collections import Iterator
# from py_db import connection
from pydbclib import connection
import pandas
import types
import sys
import logging
from pyetl.logger import log
from pyetl.utils import run_time, concat_place
from pyetl.task import TaskConfig
# pandas.set_option('display.height', 1000)
# pandas.set_option('display.max_rows', 500)
pandas.set_option('display.max_columns', 500)
pandas.set_option('display.width', 1000)


def upper(x):
    if isinstance(x, (list, tuple)):
        return tuple([i.upper() for i in x])
    else:
        return x.upper()


def _change(func):
    def wrap(x):
        try:
            rs = func(*[i for i in x])
            return pandas.Series(list(rs))
        except Exception as r:
            log.error('\nhandle fun fail input:\n%s' % (x))
            raise r
    return wrap


class Etl(object):
    _src_place = ":1"
    _dst_place = ":1"
    _task_table = 'py_script_task'
    _query_count = 1000000
    _insert_count = 100000
    _field_size = 100   # 已废弃
    _debug = False

    def config(self, cfg):
        self._src_place = getattr(cfg, 'SRC_PLACEHOLDER', self._src_place)
        self._dst_place = getattr(cfg, 'DST_PLACEHOLDER', self._dst_place)
        self._task_table = getattr(cfg, 'TASK_TABLE', self._task_table)
        self._query_count = getattr(cfg, 'QUERY_COUNT', self._query_count)
        self._insert_count = getattr(cfg, 'INSERT_COUNT', self._insert_count)
        self._field_size = getattr(
            cfg, 'CREATE_TABLE_FIELD_SIZE', self._field_size)
        self._debug = getattr(cfg, 'DEBUG', self._debug)
        if self._debug:
            log.setLevel(logging.DEBUG)
        self.src_uri = getattr(cfg, 'SRC_URI', None)
        self.dst_uri = getattr(cfg, 'DST_URI', None)
        if self.src_uri is None or self.dst_uri is None:
            log.error('没有配置数据库uri\nEXIT')
            sys.exit(1)
        # TODO: 功能改惰性连接，实现配置动态修改 (TodoReview)
        self.src_obj, self.dst_obj = self._create_obj(
            self.src_uri, self.dst_uri)
        self.db = self.dst_obj
        self.src, self.dst = self.src_obj, self.dst_obj
        self.is_config = True

    def add(self, col):
        def decorator(func):
            self.funs[upper(col)] = func
        return decorator

    def _connect(self, uri):
        '''
        创建数据源操作对象，数据库类型引用pydbc
        :param uri: 对象连接窜example(文件类型：'/home/etl/training.csv'， 数据库类型："oracle://jwdn:jwdn@local:1521/xe")
        :return: 对应数据操作对象
        '''
        if isinstance(uri, str):
            return connection(uri, debug=self._debug)
        elif isinstance(uri, dict):
            uri = uri.copy()
            if 'file' in uri:
                if self.update:
                    log.error('处理文件中数据时，update参数必须为空')
                    sys.exit(1)
                else:
                    return uri['file']
            else:
                debug = uri.get("debug", self._debug)
                uri.pop("debug", None)
                return connection(**uri, debug=debug)
        else:
            log.error("无效的数据库配置(%s)" % uri.__repr__())
            sys.exit(1)

    def _create_obj(self, src_uri, dst_uri):
        return self._connect(src_uri), self._connect(dst_uri)

    def __init__(self, src_tab, dst_tab, mapping=None, update=None, unique=None):
        self.count = (0, 0, 0)
        self._total = 0
        self.is_config = False
        self.is_saving = False
        self.src_tab = src_tab
        self.dst_tab = dst_tab
        self._setUp = []
        self._tearDown = []
        log.info('start update table: %s' % dst_tab)
        self.update = upper(update) if update else ''
        self.unique = upper(unique) if unique else ''
        self.src_mapping = {upper(i): j for i, j in mapping.items()} if mapping else {}
        self.mapping = {upper(i): upper(j) for i, j in mapping.items()} if mapping else {}
        self.funs = {}
        if self.mapping and not (self.unique in self.mapping or ((set(self.unique) & set(self.mapping.keys())) == set(self.unique))):
            log.error("unique：%s 名称错误\nEXIT" % self.unique)
            sys.exit(1)

    def _handle_field_name(self):
        """
        字段名称处理
        """
        self.src_field = []
        self.src_field_name = []
        self.src_field_dict = {}
        for i, j in self.mapping.items():
            if isinstance(j, (list, tuple)):
                self.src_field_name.extend(j)
                self.src_field_dict.update(
                    {m: "{}{}".format(i, n) for n, m in enumerate(j)})
                self.src_field.extend(
                    ["{} as {}{}".format(m, i, n) for n, m in enumerate(self.src_mapping[i])])
                self.mapping[i] = [
                    "{}{}".format(i, n) for n, m in enumerate(j)]
            else:
                self.src_field_name.append(j)
                self.src_field_dict[j] = i
                self.src_field.append("{} as {}".format(self.src_mapping[i], i))
                self.mapping[i] = i
        if self.update:
            self.update_old = self.update
            if self.update not in self.src_field_name:
                self.src_field.append("%s as etl_update_flag" % self.update)
                self.update = "etl_update_flag".upper()
            else:
                self.update = [
                    i.split(" as ") for i in self.src_field if self.update == i.split(" as ")[0].upper()
                ][0][1]
                if self.update_old != upper(self.src_mapping[self.update]):
                    log.error(
                        "update field check failed(src: %s, new: %s)\nEXIT"
                        % (self.update_old, self.update)
                    )
                    sys.exit(1)

    def _gen_sql(self, where, groupby):
        """
        生成数据查询sql语句
        :param where: sql语句中where 筛选条件
        :param groupby: sql语句中group by分组条件
        :return: sql, args
        """
        if not self.mapping:
            self.src_field = ['*']
            self.update_old = self.update
        sql = ["select {columns} from {src_tab}".format(
            columns=','.join(self.src_field), src_tab=self.src_tab)]
        args = []
        if self.update:
            sql.append("where %s is not null" % self.update_old)
            if self.last_time:
                sql.append("and %s>%s" % (self.update_old, self._src_place))
                args = [self.last_time]
            if where:
                sql.append('and (%s)' % where)
        else:
            if where:
                sql.append('where (%s)' % where)
        if groupby:
            sql.append('group by')
            sql.append(groupby)
        sql = ' '.join(sql)
        return sql, args

    def _handle_data(self, src_df):
        """
        数据处理
        :param src_df: dataframe from source data
        :return: handled dataframe by function added
        """
        data = {}
        funs = self.funs.copy()
        for i, j in self.mapping.items():
            if isinstance(j, (list, tuple)):
                merged_array = map(list, zip(*[src_df[x] for x in j]))
                if i in funs:
                    data[i] = pandas.Series(merged_array).map(funs[i]).astype('object')
                    funs.pop(i)
                else:
                    log.error("'{}'字段是一个列表需要添加处理函数\nEXIT".format(i))
                    sys.exit(1)
            else:
                if i in funs:
                    data[i] = src_df[i].map(funs[i]).astype('object')
                    funs.pop(i)
                else:
                    data[i] = src_df[i].astype('object')
        for i in funs:
            cols = list(i)
            if (isinstance(i, (tuple, list)) and
                    (set(cols) & set(self.mapping.keys()) == set(cols))):
                tmp_df = src_df[cols].apply(_change(funs[i]), axis=1)
                for idx, col in enumerate(cols):
                    data[col] = tmp_df[idx].astype('object')
            else:
                log.error("所添函数'{}'字段与实际字段{}不匹配\nEXIT".format(i, self.mapping.keys()))
                sys.exit(1)
        if self.unique:
            if self.update:
                data[self.update] = src_df[self.update].astype('object')
                tmp_df = pandas.DataFrame(data)
                sorted_df = tmp_df.sort_values(by=self.update, ascending=False)
            else:
                sorted_df = pandas.DataFrame(data)
            if isinstance(self.unique, (list, tuple)):
                self._list_unique = list(self.unique)
            elif isinstance(self.unique, str):
                self._list_unique = [self.unique]
            else:
                raise TypeError("invild unique config, must be list or str")
            tmp_df = sorted_df.drop_duplicates(self._list_unique)
            # 获取删除部分的数据
            # droped_df = sorted_df.iloc[~sorted_df.index.isin(src_df.index)]
        else:
            tmp_df = pandas.DataFrame(data)
        dst_df = tmp_df[list(self.mapping.keys())]
        return dst_df

    def _query_task_info(self, days):
        """
        查询当前任务的历史更新时间点
        """
        self.task = TaskConfig(self.src_tab, self.dst_tab)
        if self.update:
            self.job, self.last_time = self.task.query(days)
            log.info("last time: %s" % self.last_time)
        else:
            self.job, self.last_time = None, None
        self.last = self.last_time

    def _gen_df_from_source(self, where, groupby, days):
        """
        获取源数据
        """
        self._query_task_info(days)
        self._handle_field_name() if self.mapping else None
        if isinstance(self.src_obj, str):
            if self.src_tab=='csv':
                iter_df = pandas.read_csv(
                    self.src_obj,
                    chunksize=self._query_count)
            else:
                log.error("文件类型'{}'不支持".format(self.src_tab))
                sys.exit()
        else:
            sql, args = self._gen_sql(where, groupby)
            log.debug("%s, Param: %s" % (sql, args))
            iter_df = pandas.read_sql(
                sql, self.src_obj.connect,
                params=args,
                chunksize=self._query_count)
        return iter_df

    def print(self, descr, df):
        if len(df) > 5:
            log.debug("%s\n%s\n\t\t\t\t......" % (descr, df[:5]))
        else:
            log.debug("%s\n%s" % (descr, df[:5]))

    def run(self, where=None, groupby=None, days=None):
        self.save(self.transform(where, groupby, days))

    def transform(self, where=None, groupby=None, days=None):
        """
        数据处理任务执行
        :param where: 查询sql的过滤条件 example: where="id is not null"
        :param groupby: 查询sql的group by 语句
        :param days: 为0 表示全量重跑， 其他数字表示重新查询days天前的数据
        :return: 处理完成的数据 (generator object)
        """
        if not self.is_config:
            log.error('需要先加载配置文件\nEXIT')
            sys.exit(1)
        for func, param in self._setUp:
            func(param)
        iter_df = self._gen_df_from_source(where, groupby, days)
        for src_df in iter_df:
            if isinstance(self.src_obj, str):
                column_upper = [self.src_field_dict[i].upper() for i in list(src_df.columns)]
            else:
                column_upper = [i.upper() for i in list(src_df.columns)]
                if not self.mapping:
                    self.mapping = {i:i for i in column_upper}
            src_df.rename(
                columns={i: j for i, j in zip(src_df.columns, column_upper)},
                inplace=True)
            self.print('src data', src_df)
            self.print('src type', pandas.DataFrame(src_df.dtypes).T)
            if self.update:
                last_time = src_df[self.update].max()
                self.last_time = max(self.last_time, last_time
                                     ) if self.last_time else last_time
            dst_df = self._handle_data(src_df)
            # NaN值处理： 替换 NaN 为None
            dst_df = dst_df.where(dst_df.notnull(), None)
            self.print('dst type', pandas.DataFrame(dst_df.dtypes).T)
            self.print('dst data', dst_df)
            # import pdb
            # pdb.set_trace()
            yield dst_df

    @run_time
    def save(self, *args, on=''):
        """
        数据保存
        :param args: 接收要保存的数据(Iterable object)， 多组数据会依次根据on参数合并
        :param on: args为多组数据是必选，作为数据合并的依据
        """
        if self.is_saving:
            raise Exception("Etl method 'save' can not be recalled")
        flag = None
        if len(args) == 1:
            for df in args[0]:
                flag = True
                self._save_df(df)
            else:
                log.info('没有数据更新') if flag is None else None
        else:
            if not on:
                log.error("join多个dataframe需要参数on \n"
                          "example app.join(rs1, rs2, rs3, on=['id'])\nEXIT")
                sys.exit()
            args = map(next, args) if isinstance(args[0], Iterator) else args
            if len(args) == 2:
                rs = pandas.merge(args[0], args[1], on=on, how='outer')
                self._save_df(rs)
            else:
                rs = pandas.merge(args[0], args[1], on=on, how='outer')
                new_args = (rs,) + args[2:]
                self.save(*new_args, on=on)
        log.info('总数据接入数量：%s' % self.count[0])
        log.info('总数据插入数量：%s' % self.count[1])
        log.info('总数据修改数量：%s' % self.count[2])

    def _save_df(self, df):
        """
        保存数据并记录最后更新的时间点
        """
        self.is_saving = True
        total = len(df)
        if isinstance(self.dst_obj, str):
            if self.dst_tab=='csv':
                df.to_csv(self.dst_obj, index=False)
                tmp_count = total, total, 0
                self.count = [tmp_count[i]+self.count[i] for i in [0,1,2]]
            else:
                log.error("文件类型'{}'不支持".format(self.dst_tab))
                sys.exit()
        else:
            columns = list(df.columns)
            sql = "insert into %s(%s) values(%s)" % (
                self.dst_tab, ','.join(columns),
                concat_place(columns, place=self._dst_place))
            if self.last_time:
                if self.job and self.unique:
                    args = [dict(zip(columns, i)) for i in df.values]
                    self._count = self.dst_obj.merge(self.dst_tab, args, self.unique, num=self._insert_count)
                    self.task.update(self.last_time)
                else:
                    args = list(map(tuple, df.values))
                    self._count = self.dst_obj.insert(sql, args, num=self._insert_count)
                    self.task.append(self.last_time)
            else:
                args = list(map(tuple, df.values))
                if self.unique:
                    args = [dict(zip(columns, i)) for i in df.values]
                    self._count = self.dst_obj.merge(self.dst_tab, args, self.unique, num=self._insert_count)
                    self.task.append()
                else:
                    self._count = self.dst_obj.insert(sql, args, num=self._insert_count)
                    self.task.append()
            self.df = df
            for func, param in self._tearDown:
                func(param)
            self.dst_obj.commit()
            self.task.commit()
            delta = total - self._count
            tmp_count = total, self._count, delta
            log.info('单次接入: %s, %s, %s' % tmp_count)
            self.count = [tmp_count[i]+self.count[i] for i in [0,1,2]]



    def save_df(self, df, table, unique):
        args = self.df_to_dict(df)
        self.dst_obj.merge(table, args, unique, num=self._insert_count)

    def df_to_dict(self, df):
        columns = list(df.columns)
        args = [dict(zip(columns, i)) for i in df.values]
        return args

    def after(self, func):
        # self.tearDown = types.MethodType(func, self)
        self._tearDown.append((func, self))

    def before(self, func):
        # self.setUp = types.MethodType(func, self)
        self._setUp.append((func, self))

    # def setUp(self):
    #     pass

    # def tearDown(self):
    #     pass
