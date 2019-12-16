# CREATE SPACE 语法

```ngql
CREATE SPACE [IF NOT EXISTS] <space_name>
   [(partition_num = <part_num>, replica_factor = <raft_copy>)]
```

以上语句用于创建一个新的图空间。不同的图空间是物理隔离的。

## IF NOT EXISTS

创建图空间可使用 `IF NOT EXISTS` 关键字，这个关键字会自动检测对应的图空间是否存在，如果不存在则创建新的，如果存在则直接返回。

**注意：** 这里判断图空间是否存在只是比较图空间的名字(不包括属性)。

## Space Name 图空间名

* **space_name**

    图空间的名称在集群中标明了一个唯一的空间。命名规则详见 [Schema Object Names](../../3.language-structure/schema-object-names.md)

## 自定义图空间选项

在创建图空间的时候，可以传入如下两个自定义选项：

* _partition_num_

    _partition_num_ 表示数据分片数量。默认值为 100。

* _replica_factor_

    _replica_factor_ 表示副本数量。默认值是 1，集群建议为 3

如果没有自定义选项，**Nebula Graph** 会使用默认的值（partition_number 和 replica_factor）来创建图空间。

## 示例

```ngql
nebula> CREATE SPACE my_space_1; -- 使用默认选项创建图空间
nebula> CREATE SPACE my_space_2(partition_num=10); -- 使用默认 replica_factor 创建图空间
nebula> CREATE SPACE my_space_3(replica_factor=1);  -- 使用默认 partition_number 创建图空间
nebula> CREATE SPACE my_space_4(partition_num=10, replica_factor=1);
```
