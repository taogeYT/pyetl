from pyetl import Etl
from etl.config import default


def task():
    src_tab = ''
    src_update = ''
    dst_tab = ''
    dst_unique = ''
    mapping = {'': ''}
    app = Etl(src_tab, dst_tab, mapping, update=src_update, unique=dst_unique)
    app.config(default)
    app.save(app.run())
    return app.db


def main():
    task()


if __name__ == '__main__':
    main()
