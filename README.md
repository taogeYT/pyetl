# Pyetl

Pyetl is a **Python 3.6+** ETL framework

## Installation:
```shell script
pip3 install pyetl
```

## Example

```python
from pyetl import Task, DatabaseReader, DatabaseWriter, ElasticSearchWriter, HiveWriter2
db_reader = DatabaseReader("sqlite:///db.sqlite3", table_name="source_table")
db_writer = DatabaseWriter("sqlite:///db.sqlite3", table_name="target_table")
hive_writer = HiveWriter2("hive://localhost:10000/default", table_name="target_table")
es_writer = ElasticSearchWriter(hosts=["localhost"], index_name="tartget_index")

# 数据库之间数据同步，表到表传输
Task(db_reader, db_writer).start()
# 数据库到hive表同步
Task(db_reader, hive_writer).start()
# 数据库表同步es
Task(db_reader, es_writer).start()
```

#### 原始表目标表字段名称不同

```python
# 原始表source_table包含uuid，full_name字段
reader = DatabaseReader("sqlite:///db.sqlite3", table_name="source_table")
# 目标表target_table包含id，name字段
writer = DatabaseWriter("sqlite:///db.sqlite3", table_name="target_table")
# columns配置目标表和原始表的字段映射
columns = {"id": "uuid", "name": "full_name"}
Task(reader, writer, columns=columns).start()
```

#### 添加字段的map函数，对字段进行校验、做标准化、数据清洗等
```python
# functions配置字段的map函数，如下将id字段类型转换为字符串
Task(reader, writer, columns=columns, functions={"id": str}).start()
```

#### 继承Task，灵活扩展

```python
import json
from pyetl import Task, DatabaseReader, DatabaseWriter
class NewTask(Task):
    reader = DatabaseReader("sqlite:///db.sqlite3", table_name="source_table")
    writer = DatabaseWriter("sqlite:///db.sqlite3", table_name="target_table")
    
    def get_columns(self):
        """通过函数的方式生成字段映射配置，使用更灵活"""
        sql = "select columns from task where name='new_task'"
        columns = self.writer.db.read_one(sql)["columns"]
        return json.loads(columns)
      
    def get_functions(self):
        """函数方式返回要清洗字段的map函数"""
        return {"id": str, "name": self.name_func}
      
    def name_func(self, value):
        """name字段清洗函数，将name转换成首字母大写"""
				return value.capitalize()
      
    def apply_function(self, record):
        """数据流中对一整条数据执行map函数"""
        record["flag"] = int(record["id"]) % 2
        return record

    def before(self):
        """任务开始前要执行的操作"""
        sql = "create table destination_table(id int, name varchar(100))"
        self.writer.db.execute(sql)
    
    def after(self):
        """任务完成后要执行的操作"""
        sql = "update task set status='done' where name='new_task'"
        self.writer.db.execute(sql)

NewTask().start()
```

## Reader和Writer

| Reader         | 介绍                          |
| -------------- | ----------------------------- |
| DatabaseReader | 支持所有关系型数据库的读取    |
| FileReader     | 结构化文本数据读取，如csv文件 |
| ExcelReader    | Excel表文件读取               |

| Writer              | 介绍                       |
| ------------------- | -------------------------- |
| DatabaseWriter      | 支持所有关系型数据库的写入 |
| ElasticSearchWriter | 批量写入数据到es索引       |
| HiveWriter          | 批量插入hive表             |
| HiveWriter2         | Load data方式导入hive表（推荐) |
| FileWriter          | 写入数据到文本文件         |

 

