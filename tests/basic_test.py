from context import Etl
from config import config
from dateutil import parser
from env_config import env_config


def task_fromfile():
    src_tab = 'csv'
    dst_tab = 'pyetl_src'
    dst_unique = 'id'
    mapping = {'id': 'id', 'foo': 'foo', 'date_time': 'date_time',
               'x': 'x', 'y': 'y'}
    app = Etl(src_tab, dst_tab, mapping, unique=dst_unique)
    app.config(config['fromfile'])
    app.add('date_time')(lambda x: parser.parse(x) if x else None)
    app.run(days=0, where="rownum<10")


def task_oracle():
    src_tab = 'pyetl_src'
    src_update = 'date_time'
    dst_tab = 'pyetl_dst'
    dst_unique = 'id'
    mapping = {'id': 'id', 'bar': 'foo', 'update_time': 'date_time',
               'lon': 'x', 'lat': 'y'}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(config['oracle'])
    app.run(days=0, where="rownum<10")

def setup(app):
    sql = "delete from %s" % app.dst_tab
    print(sql)
    rs = app.dst.insert(sql)
    print(rs)

def task_etl():
    src_tab = 'pyetl_src'
    dst_tab = 'pyetl_src'
    app = Etl(src_tab, dst_tab, unique='id')
    app.config(config['oracle'])
    app.before(setup)
    app.add('foo')(lambda x: x+'!')
    app.run()

def task_tofile():
    src_tab = 'pyetl_src'
    dst_tab = 'csv'
    dst_unique = 'id'
    mapping = {'id': 'id', 'bar': 'foo', 'update_time': 'date_time',
               'lon': 'x', 'lat': 'y'}
    app = Etl(src_tab, dst_tab, mapping, unique=dst_unique)
    app.config(config['tofile'])
    app.run(days=0, where="rownum<10")


def main():
    with env_config():
        task_fromfile()
        task_oracle()
        task_etl()
        # import pdb;pdb.set_trace()
        task_tofile()


if __name__ == '__main__':
    main()
