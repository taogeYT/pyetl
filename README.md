# Pyetl

Pyetl is a **Python 3.6+** ETL framework

## Installation:
```shell script
pip3 install pyetl
```

## Example

```python
import sqlite3
import pymysql
from pyetl import Task, DatabaseReader, DatabaseWriter, ElasticsearchWriter, FileWriter
src = sqlite3.connect("file.db")
reader = DatabaseReader(src, table_name="source_table")
# 数据库之间数据同步，表到表传输
dst = pymysql.connect(host="localhost", user="your_user", password="your_password", db="test")
writer = DatabaseWriter(dst, table_name="target_table")
Task(reader, writer).start()
# 数据库表导出到文件
writer = FileWriter(file_path="./", file_name="file.csv")
Task(reader, writer).start()
# 数据库表同步es
writer = ElasticsearchWriter(index_name="target_index")
Task(reader, writer).start()
```

#### 原始表目标表字段名称不同

```python
import sqlite3
from pyetl import Task, DatabaseReader, DatabaseWriter
con = sqlite3.connect("file.db")
# 原始表source_table包含uuid，full_name字段
reader = DatabaseReader(con, table_name="source_table")
# 目标表target_table包含id，name字段
writer = DatabaseWriter(con, table_name="target_table")
# columns配置目标表和原始表的字段映射
columns = {"id": "uuid", "name": "full_name"}
Task(reader, writer, columns=columns).start()
```

#### 添加字段的udf映射，对字段进行规则校验、数据标准化、数据清洗等
```python
# functions配置字段的udf映射，如下id转字符串，name去除前后空格
functions={"id": str, "name": lambda x: x.strip()}
Task(reader, writer, columns=columns, functions=functions).start()
```

#### 继承Task，灵活扩展

```python
import json
from pyetl import Task, DatabaseReader, DatabaseWriter
class NewTask(Task):
    reader = DatabaseReader("sqlite:///db.sqlite3", table_name="source")
    writer = DatabaseWriter("sqlite:///db.sqlite3", table_name="target")
    
    def get_columns(self):
        """通过函数的方式生成字段映射配置，使用更灵活"""
        # 以下示例将数据库中的字段映射配置取出后转字典类型返回
        sql = "select columns from task where name='new_task'"
        columns = self.writer.db.read_one(sql)["columns"]
        return json.loads(columns)
      
    def get_functions(self):
        """通过函数的方式生成字段的udf映射"""
        # 以下示例将每个字段类型都转换为字符串
        return {col: str for col in self.columns}
      
    def apply_function(self, record):
        """数据流中对一整条数据的udf"""
        record["flag"] = int(record["id"]) % 2
        return record

    def before(self):
        """任务开始前要执行的操作, 如初始化任务表，创建目标表等"""
        sql = "create table destination_table(id int, name varchar(100))"
        self.writer.db.execute(sql)
    
    def after(self):
        """任务完成后要执行的操作，如更新任务状态等"""
        sql = "update task set status='done' where name='new_task'"
        self.writer.db.execute(sql)

NewTask().start()
```

## Reader和Writer

| Reader              | 介绍                       |
| ------------------- | -------------------------- |
| DatabaseReader      | 支持所有关系型数据库的读取    |
| FileReader          | 结构化文本数据读取，如csv文件 |
| ExcelReader         | Excel表文件读取             |
| ElasticsearchReader | 读取es索引数据    |

| Writer              | 介绍                       |
| ------------------- | -------------------------- |
| DatabaseWriter      | 支持所有关系型数据库的写入    |
| ElasticsearchWriter | 批量写入数据到es索引         |
| HiveWriter          | 批量插入hive表              |
| HiveWriter2         | Load data方式导入hive表（推荐) |
| FileWriter          | 写入数据到文本文件           |

 

