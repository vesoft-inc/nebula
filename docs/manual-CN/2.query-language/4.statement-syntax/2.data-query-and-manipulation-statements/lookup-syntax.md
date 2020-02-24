# LOOKUP 语法

`LOOKUP` 语句指定过滤条件对数据进行查询。`LOOKUP` 语句之后通常跟着 `WHERE` 子句。`WHERE` 子句用于向条件中添加过滤性的谓词，从而对数据进行过滤。

**注意：** 在使用 `LOOKUP` 语句之前，请确保已创建索引。查看[索引文档](../1.data-definition-statements/index.md)了解有关索引的更多信息。

```ngql
LOOKUP ON {<vertex_tag> | <edge_type>} WHERE <expression> [ AND | OR expression ...]) ] [YIELD <return_list>]

<return_list>
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

- `LOOKUP` 语句用于寻找点或边的集合。
- `WHERE` 指定被筛选的逻辑条件。同样支持逻辑关键词 AND、OR、NOT，详情参见 [WHERE](where-syntax.md) 的用法。
  **注意：** `WHERE` 子句在 `LOOKUP` 中暂不支持如下操作：
  - `$-` 和 `$^`
  - 在关系表达式中，暂不支持操作符两边都是field-name 的表达式，如 (tagName.column1 > tagName.column2)
  - 暂不支持运算表达式和 function 表达式中嵌套 AliasProp 表达式。
- `YIELD` 指定返回结果。如未指定，则在 `LOOKUP` 标签时返回点 ID，在 `LOOKUP` 边类型时返回边的起点 ID、终点 ID 和 ranking 值。

## 点查询

如下示例返回名称为 `Tony Parker`，标签为 _player_ 的顶点。

```ngql
nebula> CREATE TAG INDEX index_player ON player(name, age);

nebula> LOOKUP ON player WHERE player.name == "Tony Parker";
============
| VertexID |
============
| 101      |
------------

nebula> LOOKUP ON player WHERE player.name == "Tony Parker" \
YIELD person.name, person.age;
=======================================
| VertexID | player.name | player.age |
=======================================
| 101      | Tony Parker | 36         |
---------------------------------------

nebula> LOOKUP ON player WHERE player.name== "Kobe Bryant" YIELD player.name AS name | \
GO FROM $-.VertexID OVER serve YIELD $-.name, serve.start_year, serve.end_year, $$.team.name;
==================================================================
| $-.name     | serve.start_year | serve.end_year | $$.team.name |
==================================================================
| Kobe Bryant | 1996             | 2016           | Lakers       |
------------------------------------------------------------------
```

## 边查询

如下示例返回 `degree` 为 90，边类型为 _follow_ 的边。

```ngql
nebula> CREATE EDGE INDEX index_follow ON follow(degree);

nebula> LOOKUP ON follow WHERE follow.degree == 90;
=============================
| SrcVID | DstVID | Ranking |
=============================
| 100    | 106    | 0       |
-----------------------------

nebula> LOOKUP ON follow WHERE follow.degree == 90 YIELD follow.degree;
=============================================
| SrcVID | DstVID | Ranking | follow.degree |
=============================================
| 100    | 106    | 0       | 90            |
---------------------------------------------

nebula> LOOKUP ON follow WHERE follow.degree == 60 YIELD follow.degree AS Degree | \
GO FROM $-.DstVID OVER serve YIELD $-.DstVID, serve.start_year, serve.end_year, $$.team.name;
================================================================
| $-.DstVID | serve.start_year | serve.end_year | $$.team.name |
================================================================
| 105       | 2010             | 2018           | Spurs        |
----------------------------------------------------------------
| 105       | 2009             | 2010           | Cavaliers    |
----------------------------------------------------------------
| 105       | 2018             | 2019           | Raptors      |
----------------------------------------------------------------
```
