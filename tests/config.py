class TestingConfig:
    DEBUG = True
    # SRC_URI = {"uri": "DSN=mydb;UID=root;PWD=password", 'driver': 'pyodbc'}
    SRC_URI = {"uri": "DSN=mysqldb", 'driver': 'pyodbc'}
    DST_URI = "oracle://lyt:lyt@local:1521/xe"
    SRC_PLACEHOLDER = "?"
    # DST_PLACEHOLDER = ":1"
    # TASK_TABLE = 'task'
    # QUERY_COUNT = 2000000
    # INSERT_COUNT = 200000
    # CREATE_TABLE_FIELD_SIZE = 200


class TestoracleConfig:
    DEBUG = True
    QUERY_COUNT = 200
    SRC_URI = {"uri": "oracle://lyt:lyt@local:1521/xe", "debug": True}
    DST_URI = {"uri": "oracle://lyt:lyt@local:1521/xe", "debug": False}


class TestFromfileConfig:
    DEBUG = True
    SRC_URI = {'file': 'pyetl_src.csv'}
    DST_URI = "oracle://lyt:lyt@local:1521/xe"


class TestTofileConfig:
    DEBUG = True
    SRC_URI = "oracle://lyt:lyt@local:1521/xe"
    DST_URI = {'file': 'pyetl.csv'}


class DevelopmentConfig:
    pass


config = {
    'testing': TestingConfig,
    'fromfile': TestFromfileConfig,
    'tofile': TestTofileConfig,
    'oracle': TestoracleConfig,
    'default': TestoracleConfig
}
