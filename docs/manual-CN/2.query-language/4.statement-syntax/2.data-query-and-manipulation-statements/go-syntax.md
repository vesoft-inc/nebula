# GO 语法

`GO` 是 **Nebula Graph** 中最常用的关键字，可以指定过滤条件（如 `WHERE`）遍历图数据并获取点和边的属性，还能以指定顺序（`ORDER BY ASC | DESC`）返回指定数目（`LIMIT`）的结果。

>`GO` 的用法与 SQL 中的 `SELECT` 类似，重要区别是 `GO` 必须从遍历一系列的节点开始。

```ngql
  GO [ <N> STEPS ] FROM <node_list>
  OVER <edge_type_list> [REVERSELY] [BIDIRECT]
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

* [ \<N> STEPS ] 指定查询 N 跳。
* <node_list> 为逗号隔开的节点 ID，或特殊占位符 `$-.id` (参看 `PIPE` 用法)。
* <edge_type_list> 为图遍历返回的边类型列表。
* [ WHERE \<expression> ] 指定被筛选的逻辑条件，WHERE 可用于起点，边及终点，同样支持逻辑关键词 AND、OR、NOT，详情参见 WHERE 的用法。
* YIELD [DISTINCT] <return_list> 以列的形式返回结果，并可对列进行重命名。详情参看 `YIELD` 用法。`DISTINCT` 的用法与 SQL 相同。

## 示例

```ngql
nebula> GO FROM 107 OVER serve;  \
   /* 从点 107 出发，沿边 serve，找到点 200，201 */
==============
| serve._dst |
==============
| 200        |
--------------
| 201        |
--------------
```

```ngql
nebula> GO 2 STEPS FROM 103 OVER follow; \
  /* 返回点 103 的 2 度的好友 */
===============
| follow._dst |
===============
| 101         |
---------------
```

```ngql
nebula> GO FROM 109 OVER serve  \
   WHERE serve.start_year > 1990       /* 筛选边 serve 的 start_year 属性  */ \
   YIELD $$.team.name AS team_name, serve.start_year as start_year;    /* 目标点 team 的 serve.start_year 属性 serve.start_year */
==========================
| team_name | start_year |
==========================
| Nuggets   | 2011       |
--------------------------
| Rockets   | 2017       |
--------------------------
```

```ngql
nebula> GO FROM 100,102 OVER serve           \
        WHERE serve.start_year > 1995             /* 筛选边属性*/ \
        YIELD DISTINCT $$.team.name AS team_name, /* DISTINCT 与 SQL 用法相同 */ \
        serve.start_year as start_year,           /* 边属性 */ \
        $^.player.name AS player_name;            /* 起点 (player) 属性 */
==============================================
| team_name | start_year | player_name       |
==============================================
| Warriors  | 2001       | LaMarcus Aldridge |
----------------------------------------------
| Warriors  | 1997       | Tim Duncan        |
----------------------------------------------
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

> 请注意，当沿着多种类型边遍历时，对于使用过滤条件有特别限制(也即 `WHERE` 语句），比如 `WHERE edge1.prop1 > edge2.prop2` 这种过滤条件是不支持的。

对于返回的结果，如果存在多条边的属性需要返回，会把他们放在不同的行。比如：

```ngql
nebula> GO FROM 100 OVER follow, serve YIELD follow.degree, serve.start_year;

返回如下结果：
====================================
| follow.degree | serve.start_year |
====================================
| 0             | 1997             |
------------------------------------
| 95            | 0                |
------------------------------------
| 89            | 0                |
------------------------------------
| 90            | 0                |
------------------------------------

没有的属性当前会填充默认值， 数值型的默认值为 0， 字符型的默认值为空字符串。bool 类型默认值为 false，timestamp 类型默认值为 0 (即 "1970-01-01 00:00:00")，double 类型默认值为 0.0。

当然也可以不指定 `YIELD`， 这时会返回每条边目标点的 vid。如果目标点不存在，同样用默认值(此处为 0)填充。比如 `GO FROM 100 OVER follow, serve;`，返回结果如下：

============================
| follow._dst | serve._dst |
============================
| 0           | 200        |
----------------------------
| 101         | 0          |
----------------------------
| 102         | 0          |
----------------------------
| 106         | 0          |
----------------------------
```

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
nebula> GO FROM 100 OVER follow REVERSELY YIELD follow._src -- 返回 100
```

```ngql
nebula> GO FROM 100 OVER follow REVERSELY YIELD follow._dst AS id | \
        GO FROM $-.id OVER serve WHERE $^.player.age > 20 YIELD $^.player.name AS FriendOf, $$.team.name AS Team;

============================
| FriendOf      | Team     |
============================
| Tony Parker   | Warriors |
----------------------------
| Kyle Anderson | Warriors |
----------------------------
```

遍历所有关注 100 号球员的球员，找出这些球员服役的球队，筛选年龄大于 20 岁的球员并返回这些球员姓名和其服役的球队名称。如果此处不指定 `YIELD`，则默认返回每条边目标点的 `vid`。

## 双向遍历

目前 **Nebula Graph** 支持使用关键词 `BIDIRECT` 进行双向遍历，语法为：

```ngql
  GO FROM <node_list>
  OVER <edge_type_list> BIDIRECT
  WHERE (expression [ AND | OR expression ...])  
  YIELD | YIELDS  [DISTINCT] <return_list>
```

例如：

```ngql
nebula> GO FROM 102 OVER follow BIDIRECT
===============
| follow._dst |
===============
| 101         |
---------------
| 103         |
---------------
| 135         |
---------------
```

上述语句同时返回 102 关注的球员及关注 102 的球员。
