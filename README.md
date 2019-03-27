## python etl frame for small dataset

### Installation:
    pip install pyetl

### create project
    pyetl -b [project name]
    以上命令创建好一个工程文件，需要根据个人环境进行配置
    app/config.py文件(主要是SRC_URI和DST_URI参数)
        SRC_URI(源库配置)
        DST_URI(目的库配置)
        SRC_PLACEHOLDER(数据源驱动的占位符形式， 以sqlalchemy连接形式配置时不需要关注)
        QUERY_SIZE(单次获取数据量，根据实际环境机器性能，可以不关注)
        INSERT_SIZE(单次插入数据量，根据实际环境机器性能，可以不关注)

    app/etl/job.py文件，单个etl任务示例(主要是src_tab和dst_tab参数)
        src_tab(源表名称)
        dst_tab(目的表名称)
        mapping(可选关键字参数，目的表到源表的字段映射，当源表和目的表字段名称不一致是使用)
        src_update(可选关键字参数，源表的数据更新标志，只要是增长类型的都可以，比如数据变更时间字段，非空以增量形式插入)
        dst_unique(可选关键字参数，目的表的唯一键，非空以merge形式插入)


#### usage by a script:

    from pyetl import Etl
    class TestOracleConfig:
        DST_URI = SRC_URI = {"uri": "mysql+pymysql://root:hadoop@localhost:3306/test"}

    app = Etl('test_src', 'test_dst')
    app.config(TestOracleConfig)
    app.run()

#### utf and other functions

    app = Etl('src_table', 'dst_table', unique='id')
    app.config(TestOracleConfig)

    @app.add('id') # add 函数可以向任务注册某个或某几个字段的转换函数
    def udf(x):
        return x.upper()
    @app.after     # after 函数是在一次etl完成后的扫尾操作，类似的函数还有before
    def clearup(app):
        print(app.src.read("select count(*) from dst_table"))
        app.dst.empty('src_table')

    app.run(where="rownum<10") # 数据源过滤

### 关于Config
数据库源按[pydbclib](https://github.com/taogeYT/pydbclib)配置标准