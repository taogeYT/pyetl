# Pyetl

Pyetl is a **Python 3.6+** ETL framework

## Installation:
```shell script
pip install pyetl
```

## Usage:
#### Quick start
```python
from pydbclib import connect
from pyetl import Task, DatabaseReader, DatabaseWriter
# pydbclib创建通用数据库操作
db = connect("mysql://user:password@localhost:3306/test")
# DatabaseReader有个condition可选参数用来添加条件过滤原始数据
reader = DatabaseReader(db, table_name="source_table") # 从source_table表获取数据流
writer = DatabaseWriter(db, table_name="destination_table") # 数据流写入destination_table表
# columns参数是目标表到原始表的字段映射
# functions参数是对映射字段添加map处理函数，如下map函数将id字段类型转换为字符串
task = Task(reader, writer, columns={"id", "name"}, functions={"id": str})
task.start()
```



#### 原始表目标表字段名称不同

```python
from pydbclib import connect
from pyetl import Task, DatabaseReader, DatabaseWriter
db = connect("mysql://user:password@localhost:3306/test")
# 原始表source_table包含uuid，full_name字段
reader = DatabaseReader(db, table_name="source_table")
# 目标表destination_table包含id，name字段
writer = DatabaseWriter(db, table_name="destination_table")
# 配置原始表到目标表的字段映射
columns={"id": "uuid", "name": "full_name"}
task = Task(reader, writer, columns=columns, functions={"id": str})
task.start()
```

#### 继承Task，灵活扩展

```python
import json
from pydbclib import connect
from pyetl import Task, DatabaseReader, DatabaseWriter
db = connect("mysql://user:password@localhost:3306/test")
# 配置原始表到目标表的字段映射
columns={"id": "uuid", "name": "full_name"}
class NewTask(Task):
    reader = DatabaseReader(db, table_name="source_table")
    writer = DatabaseWriter(db, table_name="destination_table")
    
    def get_columns(self):
        """通过函数的方式生成字段映射配置，使用更灵活"""
        sql = "select columns from task where name='new_task'"
        columns = self.writer.db.read_one(sql)["columns"]
        return json.loads(columns)
      
    def apply_function(self, record):
        """数据流中对一个整条数据执行map函数"""
        record["flag"] = 1 if record["id"] % 2 else 0
        return record

    def before(self):
        sql = "create table destination_table(id int, name varchar(100))"
				self.writer.db.execute(sql)
    
    def after(self):
        """任务完成后要执行的操作"""
        sql = "update task set status='done' where name='new_task'"
        self.writer.db.execute(sql)

NewTask().start()
```



#### reader和writer支持

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
| HiveWriter2         | Load data方式写入hive表    |
| FileWriter          | 写入数据到文本文件         |

 

