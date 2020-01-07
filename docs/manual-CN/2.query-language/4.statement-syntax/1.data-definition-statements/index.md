# Schema 索引

```ngql
CREATE {TAG | EDGE} INDEX [IF NOT EXISTS] <index_name> ON {<tag_name> | <edge_name>} (prop_name_list)
```

schema 索引可用于快速处理图查询。**Nebula Graph** 支持两种类型的索引：**标签索引**和**边类型索引**。

多数图查询都从拥有共同属性的同一类型的点或边开始遍历。schema 索引使得这些全局检索操作在大型图上更为高效。

一般地，在使用 `CREATE TAG/EDGE` 语句将标签或边类型创建好之后，即可为其创建索引。

## 创建索引

`CREATE INDEX` 用于为已有标签或边类型创建索引。

### 创建单属性索引

```ngql
nebula> CREATE TAG INDEX player_index_0 on player(name);
```

上述语句在所有标签为 _player_ 的顶点上为属性 _name_ 创建了一个索引。

```ngql
nebula> CREATE EDGE INDEX follow_index_0 on follow(degree);
```

上述语句在 _follow_ 边类型的所有边上为属性 _degree_ 创建了一个索引。

### 创建组合索引

schema 索引还支持为多个属性同时创建索引。这种包含多种属性的索引在 **Nebula Graph** 中称为复合索引。

```ngql
nebula> CREATE TAG INDEX player_index_1 on player(name,age);
```

上述语句在所有标签为 _player_ 的顶点上为属性 _name_ 和 _age_ 创建了一个复合索引。

## 列出索引

```ngql
SHOW {TAG | EDGE} INDEXES
```

`SHOW INDEXES` 用于列出已创建完成的标签或边类型的索引信息。使用以下命令列出索引：

```ngql
nebula> SHOW TAG INDEXES;
=============================
| Index ID | Index Name     |
=============================
| 22       | player_index_0 |
-----------------------------
| 23       | player_index_1 |
-----------------------------

nebula> SHOW EDGE INDEXES;
=============================
| Index ID | Index Name     |
=============================
| 24       | follow_index_0 |
-----------------------------

```

## 返回索引信息

```ngql
DESCRIBE {TAG | EDGE} INDEX <index_name>
```

`DESCRIBE INDEX` 用于返回指定索引信息。例如，使用以下命令返回索引信息：

```ngql
nebula> DESCRIBE TAG INDEX player_index_0;
==================
| Field | Type   |
==================
| name  | string |
------------------

nebula> DESCRIBE TAG INDEX player_index_1;
==================
| Field | Type   |
==================
| name  | string |
------------------
| age   | int    |
------------------
```

## 删除索引

```ngql
DROP {TAG | EDGE} INDEX [IF EXISTS] <index_name>
```

`DROP INDEX` 用于删除指定名称的标签或边类型索引。例如，使用以下命令删除名为 _player_index_0_ 的索引：

```ngql
nebula> DROP TAG INDEX player_index_0;
```
