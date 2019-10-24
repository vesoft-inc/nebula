# CREATE SPACE 语法

```
CREATE SPACE space_name
    (space_option,...)

space_option:
    option_name = value
```

以上语句作用是创建一个新的图空间。不同的图空间是物理隔离的。 

## Space Name 图空间名

* **space_name**

    图空间的名称在集群中标明了一个唯一的空间。命名规则详见 [Schema Object Names](../../3.language-structure/schema-object-names.md)

### Space Options 图空间选项

在创建图空间的时候，可以传入自定义选项。选项名称 _option_name_ 可以是以下任何一个：
* _partition_num_

    _partition_num_ 表示数据分片数量。默认值为 1024。

* _replica_factor_

    _replica_factor_ 表示副本数量。默认值是 1, 集群建议为 3

如果没有自定义选项，Nebula 会使用默认的值（partition_number 和 replica_factor）来创建图空间。

### 示例

```
CREATE SPACE my_space_1; -- 使用默认选项创建图空间
CREATE SPACE my_space_2(partition_num=100); -- 使用默认 replica_factor 创建图空间
CREATE SPACE my_space_3(replica_factor=1);  -- 使用默认 partition_number 创建图空间
CREATE SPACE my_space_4(partition_num=100, replica_factor=1);
```

