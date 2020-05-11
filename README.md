# Python ETL Framework

pyetl is a ETL Framework for small dataset

## Installation:
```shell script
pip install pyetl
```

## A Simple Example:
确保数据库'foo'和'bar'表已创建
foo表插入以下样例数据：

| uuid | name | age  |
| ---- | ---- | ---- |
| 1    | 张三 | 15   |
| 2    | 李四 | 20   |
| 3    | 王五 | 25   |

bar表是包含uuid和name字段的空表

下面开始一个简单的etl任务，将foo表id和full_name两列数据插入bar表


```python
from pyetl import Task, Reader, Writer, Mapping
uri = "mysql://user:password@localhost:3306/test"
# Reader和Writer的前两个参数是数据库连接和表名称
reader = Reader(uri, "foo") # Reader有个condition可选参数用来添加条件过滤原始数据
writer = Writer(uri, "bar")
# columns参数是目标表到原始表的字段映射, 目标表和原始表字段名称相同可以写成集合形式如{"uuid", "name"}， functions参数是对映射字段添加map处理函数
mapping = Mapping(columns={"id": "uuid", "full_name": "name"}, functions={"id": lambda x: f"id-{x}"})
task = Task(reader, mapping, writer)
task.start()
```
