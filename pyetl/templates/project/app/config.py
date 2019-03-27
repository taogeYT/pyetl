# -*- coding: utf-8 -*-

class TestConfig:
    DEBUG = True
    SRC_URI = "mysql+pymysql://root:hadoop@localhost:3306/test"
    DST_URI = SRC_URI
    # SRC_PLACEHOLDER = "?"
    # TASK_TABLE = 'py_script_task'
    # QUERY_SIZE = 2000000
    # INSERT_SIZE = 200000
    # NEW_TABLE_FIELD_DEFAULT_SIZE = 200

class DevConfig:
    pass

cfg = {
    'test': TestConfig,
    "product": DevConfig,
    'default': TestConfig
}

default = cfg['default']
