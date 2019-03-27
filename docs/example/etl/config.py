class TestConfig:
    DEBUG = True
    SRC_URI = "oracle://jwdn:password@local:1521/xe"
    DST_URI = SRC_URI
    # SRC_PLACEHOLDER = "?"
    # TASK_TABLE = 'py_script_task'
    # QUERY_SIZE = 2000000
    # INSERT_SIZE = 200000
    # NEW_TABLE_FIELD_DEFAULT_SIZE = 200


class DevConfig:
    """
    警务大脑项目
    """
    DEBUG = True
    SRC_URI = "oracle://zzjwdn:zzjwdn@172.16.11.120:1521/dcenter"
    DST_URI = "oracle://jwdn:jwdn_admin@172.16.11.144:1521/jwdn"


class PpsConfig:
    """
    警务大脑项目
    """
    DEBUG = True
    SRC_URI = "oracle://PPS1DBA:Peptalk123@172.16.11.58:1521/p570b"
    DST_URI = "oracle://PPS1DBA:Peptalk123@172.16.11.58:1521/p570b"


cfg = {
    'testing': TestConfig,
    "product": DevConfig,
    'default': TestConfig
}

default = cfg['default']
