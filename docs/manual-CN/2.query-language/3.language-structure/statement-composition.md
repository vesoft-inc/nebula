# 语法组合

组合语法（或子查询）的方法有两种：

* 将多个语法放在一个语法中进行批处理，以英文分号（;）隔开，最后一个语法的结果将作为批处理的结果返回。
* 将多个语法通过运算符（|）连接在一起，类似于shell脚本中的管道。前一个语法得到的结果可以重定向到下一个语法作为输入。

> 注意复合语法并不能保证`事务性`。
> 例如，由三个子查询组成的语法：A | B | C，其中A是读操作，B是计算，C是写操作。
> 如果任何部分在执行中失败，整个结果将未被定义--尚不支持调用回滚。

## 示例

### 分号复合语法

```sql
nebula> SHOW TAGS; SHOW EDGES;          -- 仅列出边
  INSERT VERTEX player(name, age) VALUES 100:("Tim Duncan", 42); \
  INSERT VERTEX player(name, age) VALUES 101:("Tony Parker", 36); \
  INSERT VERTEX player(name, age) VALUES 102:("LaMarcus Aldridge", 33);  /* 通过复合语法插入多个点*/
```

### 管道复合语法

```sql
nebula> GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...
```

占位符`$-.id`获取第一个语法`GO FROM 201 OVER edge_serve`返回的结果。
