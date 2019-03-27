import os
import sys
sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)
from pyetl import Etl


def eng_test():
    import os
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    eng = create_engine(
        "oracle+cx_oracle://jwdn:password@local:1521/xe", echo=True
    )
    # rs = eng.execute("insert into test(id) values(4444)")
    # print(rs.fetchall())
    DB_Session = sessionmaker(bind=eng)
    session = DB_Session()
    session.execute("insert into test(id) values(:id)", [{'id': 5555}])
    session.commit()


def pandas_test():
    import pandas
    # import cx_Oracle
    # conn = cx_Oracle.connect('jwdn/password@local:1521/xe')
    import datetime
    from sqlalchemy import create_engine
    eng = create_engine(
        "oracle+cx_oracle://jwdn:password@local:1521/xe", echo=True)
    sql = 'select sgdh,fssj,sgdddm from ACCIDENT where rownum<:1'
    # sql = 'SELECT * FROM ETL_TEST_SRC where dtime>:1'
    df = pandas.read_sql(sql, eng, params=[5])

    # dataframe类型转换
    # df['id'] = df['id'].astype('int')
    # df['length'] = df['length'].astype('float64')
    # print(len(df))

    # dataframe排序
    # print(df.sort_index(by='FSSJ', ascending=False))
    # columns = list(df.columns)
    # rs = [dict(zip(columns, i)) for i in df.values]
    # print(rs)
    # print(type(df.values))
    return df


def func(x):
    return x + '1'


if __name__ == "__main__":
    # eng_test()
    # db_test()
    df = pandas_test()
