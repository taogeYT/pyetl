# Python ETL Frame

pyetl is a etl frame for small dataset

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

下面开始一个简单的etl任务，将foo表uuid和name两列数据插入bar表


```python
from pyetl import Task, Reader, Writer, Mapping
uri = "mysql://user:password@localhost:3306/test"
# Reader和Writer的前两个参数是数据库连接和表名称
reader = Reader(uri, "foo") # Reader有个condition可选参数用来添加条件过滤原始数据
writer = Writer(uri, "bar")
# columns参数是目标表和原始表的字段映射， functions参数是对映射字段添加map处理函数
mapping = Mapping(columns={"uuid", "name"}, functions={"uuid": lambda x: f"uuid-{x}"})
task = Task(reader, mapping, writer)
task.start()
```
