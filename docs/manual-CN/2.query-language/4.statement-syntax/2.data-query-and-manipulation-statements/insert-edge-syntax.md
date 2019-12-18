# INSERT EDGE 语法

```ngql
INSERT EDGE <edge_name> ( <prop_name_list> ) VALUES | VALUE
<src_vid> -> <dst_vid>[@<ranking>] : ( <prop_value_list> )
[, <src_vid> -> <dst_vid> : ( <prop_value_list> ), ...]

<prop_name_list>:
  [ <prop_name> [, <prop_name> ] ...]

<prop_value_list>:
  [ <prop_value> [, <prop_value> ] ...]
```

INSERT EDGE 用于插入从起点（ src_vid ）到终点（ dst_vid ）的一条边。

* `<edge_name>` 表示边类型，在进行 `INSERT EDGE` 操作前需创建好。
* `<prop_name_list>` 为指定边的属性列表。
* `<prop_value_list>` 须根据 <prop_name_list> 列出属性，如无匹配类型，则返回错误。
* `ranking` 指定边 ranking，可在插入同一类型的多条边时使用，可选，不指定时默认为 0。

> 目前版本尚不支持默认属性值。

## 示例

```ngql
nebula> CREATE EDGE e1()                    -- 创建空属性边 t1
nebula> INSERT EDGE e1 () VALUES 10->11:()    -- 插入一条从点 10 到点 11 的空属性边
nebula> INSERT EDGE e1 () VALUES 10->11@1:()  -- 插入一条从点 10 到点 11 的空属性边，ranking 值为 1
```

```ngql
nebula> CREATE EDGE e2 (name string, age int)                     -- 创建有两种属性的边 e2
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 1)          -- 插入一条从点 11 到点 13 的有两条属性的边
nebula> INSERT EDGE e2 (name, age) VALUES \
12->13:("n1", 1), 13->14("n2", 2)                           -- 插入两条边
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", "a13")      -- 错误操作，"a13" 不是 int 类型
```

同一条边可被多次插入或写入，读取时以最后一次插入为准。

```ngql
-- 为插入边赋新值
insert edge with new version of values.
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 12)
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 13)
nebula> INSERT EDGE e2 (name, age) VALUES 11->13:("n1", 14) -- 读取最后插入的值
```
