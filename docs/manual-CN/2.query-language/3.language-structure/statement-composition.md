# 语句组合

组合语句（或子查询）的方法有两种：

* 将多个语句放在一个语句中进行批处理，以英文分号（;）隔开，最后一个语句的结果将作为批处理的结果返回。
* 将多个语句通过运算符（|）连接在一起，类似于 shell 脚本中的管道。前一个语句得到的结果可以重定向到下一个语句作为输入。

> 注意复合语句并不能保证`事务性`。
> 例如，由三个子查询组成的语句：A | B | C，其中 A 是读操作，B 是计算，C 是写操作。
> 如果任何部分在执行中失败，整个结果将未被定义--尚不支持调用回滚。

## 示例

### 分号复合语句

```ngql
nebula> SHOW TAGS; SHOW EDGES;          -- 仅列出边
  INSERT VERTEX player(name, age) VALUES 100:("Tim Duncan", 42); \
  INSERT VERTEX player(name, age) VALUES 101:("Tony Parker", 36); \
  INSERT VERTEX player(name, age) VALUES 102:("LaMarcus Aldridge", 33);  /* 通过复合语句插入多个点*/
```

### 管道复合语句

```ngql
nebula> GO FROM 201 OVER edge_serve | GO FROM $-.id OVER edge_fans | GO FROM $-.id ...
```

占位符 `$-.id` 获取第一个语句 `GO FROM 201 OVER edge_serve YIELD edge_serve._dst AS id` 返回的结果。
