# GO 语法

`GO` 是 **Nebula Graph** 中最常用的关键字，可以指定过滤条件（如`WHERE`）遍历图数据并获取点和边的属性，还能以指定顺序（`ORDER BY ASC | DESC`）返回指定数目（`LIMIT`）的结果。

>`GO` 的用法与 SQL 中的 `SELECT` 类似，重要区别是 `GO` 必须从遍历一系列的节点开始。
<!-- >请参考`FIND`的用法，它对应于SQL中的`SELECT`。 -->

```ngql
  GO [ <N> STEPS ] FROM <node_list>
  OVER <edge_type_list> [REVERSELY]
  [ WHERE <expression> [ AND | OR expression ...]) ]
  YIELD | YIELDS [DISTINCT] <return_list>

<node_list>
   | <vid> [, <vid> ...]
   | $-.id

<edge_type_list>
   edge_type [, edge_type ...]

<return_list>
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

* [ <N> STEPS ] 指定查询 N 跳
* <node_list> 为逗号隔开的节点 ID，或特殊占位符 `$-.id` (参看 `PIPE` 用法)。
* <edge_type_list> 为图遍历返回的边类型列表。
* [ WHERE <expression> ] 指定被筛选的逻辑条件，WHERE 可用于起点，边及终点，同样支持逻辑关键词 AND、OR、NOT，详情参见 WHERE 的用法。
* YIELD [DISTINCT] <return_list> 以列的形式返回结果，并可对列进行重命名。详情参看 `YIELD`
 用法。`DISTINCT` 的用法与 SQL 相同。

## 示例

```ngql
nebula> GO FROM 101 OVER serve  \
   /* 从点 101 出发，沿边 serve，找到点 204，215 */
=======
| id  |
=======
| 204 |
-------
| 215 |
-------
```

```ngql
nebula> GO 2 STEPS FROM 103 OVER follow \
  /* 返回点 103 的 2 度的好友 */
===============
| follow._dst |
===============
| 100         |
---------------
| 101         |
---------------
```

```ngql
nebula> GO FROM 101 OVER serve  \
   WHERE serve.start_year > 1990       /* 筛选边 serve 的 start_year 属性  */ \
   YIELD $$.team.name AS team_name    /* 目标点 team 的 serve.start_year 属性 serve.start_year */
================================
| team_name | serve.start_year |
================================
| Spurs     | 1999             |
--------------------------------
| Hornets   | 2018             |
--------------------------------
```

```ngql
nebula> GO FROM 100,102 OVER serve           \
        WHERE serve.start_year > 1995             /* 筛选边属性*/ \
        YIELD DISTINCT $$.team.name AS team_name, /* DISTINCT 与 SQL 用法相同 */ \
        serve.start_year,                         /* 边属性 */ \
        $^.player.name AS player_name             /* 起点 (player) 属性 */
========================================================
| team_name     | serve.start_year | player_name       |
========================================================
| Trail Blazers | 2006             | LaMarcus Aldridge |
--------------------------------------------------------
| Spurs         | 2015             | LaMarcus Aldridge |
--------------------------------------------------------
| Spurs         | 1997             | Tim Duncan        |
--------------------------------------------------------
```

## 沿着多种类型的边进行遍历

目前 **Nebula Graph** 还支持 `GO` 沿着多条边遍历，语法为：

```ngql
GO FROM <node_list> OVER <edge_type_list | *> YIELD | YIELDS [DISTINCT] <return_list>
```

例如：

```ngql
nebula> GO FROM <node_list> OVER edge1, edge2....  //沿着 edge1 和 edge2 遍历，或者
nebula> GO FROM <node_list> OVER *    //这里 * 意味着沿着任意类型的边遍历
```

> 请注意，当沿着多种类型边遍历时，对于使用过滤条件有特别限制(也即 WHERE 语句），比如 `WHERE edge1.prop1 > edge2.prop2` 这种过滤条件是不支持的。

对于返回的结果，如果存在多条边的属性需要返回，会把他们放在不同的行。比如：

```ngql
nebula> GO FROM 100 OVER edge1, edge2 YIELD edge1.prop1, edge2.prop2
```

 如果 100 这个顶点存在 3 条类型为 edge1 的边， 2 条类型为 edge2 的边，最终的返回结果会有 5 行，如下所示：

| edge1._prop1 | edge2._prop2 |
| --- | --- |
| 10 | "" |
| 20 | "" |
| 30 | "" |
| 0 | "nebula" |
| 0 | "vesoft" |

没有的属性当前会填充默认值， 数值型的默认值为 0， 字符型的默认值为空字符串。bool 类型默认值为 false，timestamp 类型默认值为 0 (即 "1970-01-01 00:00:00")，double 类型默认值为 0.0。

当然也可以不指定 `YIELD`， 这时会返回每条边目标点的 vid。如果目标点不存在，同样用默认值(此处为 0)填充。比如 `GO FROM 100 OVER edge1, edge2`，返回结果如下：

| edge1._dst | edge2._dst |
| --- | --- |
| 101 | 0 |
| 102 | 0 |
| 103 | 0 |
| 0 | 201 |
| 0 | 202 |

对于 `GO FROM 100 OVER *` 这样的例子来说，返回结果也和上面例子类似：不存在的属性或者 vid 使用默认值来填充。
请注意从结果中无法分辨每一行属于哪条边，未来版本会在结果中把 edge type 表示出来。

## 反向遍历

目前 **Nebula Graph** 支持使用关键词 `REVERSELY` 进行反向遍历，语法为：

```ngql
  GO FROM <node_list>
  OVER <edge_type_list> REVERSELY
  WHERE (expression [ AND | OR expression ...])  
  YIELD | YIELDS  [DISTINCT] <return_list>
```

例如：

```ngql
nebula> GO FROM 125 OVER follow REVERSELY YIELD follow._src AS id | \
        GO FROM $-.id OVER serve WHERE $^.player.age > 35 YIELD $^.player.name AS FriendOf, $$.team.name AS Team

=========================
| FriendOf    | Team    |
=========================
| Tim Duncan  | Spurs   |
-------------------------
| Tony Parker | Spurs   |
-------------------------
| Tony Parker | Hornets |
-------------------------
```

遍历 follow 125 号球员的所有球员，找出这些球员服役的球队，筛选年龄大于 35 岁的球员并返回这些球员姓名和其服役的球队名称。如果此处不指定 `YIELD`，则默认返回每条边目标点的 `vid`。
