from pyetl import Etl
from app.config import default
from app.logger import module_log
log = module_log(__file__)


def task():
    src_tab = "test_src"
    # src_update = "SRC_UPDATE_FIELD"
    dst_tab = "test_dst"
    # dst_unique = "DST_UNIQUE"
    # mapping = {
    #     "ID": "CODE"
    # }
    app = Etl(src_tab, dst_tab,
            #   mapping=mapping,
            #   updte=src_update,
            #   unique=dst_unique
    )
    app.config(default)
    app.run()


def main():
    task()


if __name__=="__main__":
    main()
