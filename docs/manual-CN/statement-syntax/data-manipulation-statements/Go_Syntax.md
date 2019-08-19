`GO`是Nebula中最常用的关键字，表示以指定过滤条件（如`WHERE`）遍历图数据并获取点和边的属性，
以指定顺序（`ORDER BY ASC | DESC`）返回指定数目（`LIMIT`）结果。


>`GO`和`FIND`的用法与SQL中的`SELECT`类似，重要区别是`GO`必须从遍历一系列的节点开始。

>请参考`FIND`的用法，它对应于SQL中的`SELECT`。

```
GO FROM <node_list>
OVER <edge_type_list>
WHERE (expression [ AND | OR expression ...])  
YIELD | YIELDS  [DISTINCT] <return_list>

<node_list>
   | vid [, vid ...]
   | $-.id

<edge_type_list>
   edge_type [, edge_type ...]

<return_list>   
    <col_name> [AS <col_alias>] [, <col_name> [AS <col_alias>] ...]
```

* <node_list>为逗号隔开的节点id，或特殊占位符`$-.id`(参看`PIPE`用法)。
* <edge_type_list>为图遍历返回的边类型列表。
* WHERE <filter_list> 指定被筛选的逻辑条件，WHERE可用于起点，边及终点，同样支持逻辑关键词
AND，OR，NOT，详情参见WHERE的用法。
* YIELD [DISTINCT] <return_list>以列的形式返回结果，并可对列进行重命名。详情参看`YIELD`
用法。`DISTINCT`的用法与SQL相同。

### 示例

```
nebula> GO FROM 101 OVER serve  \
   /* 从点101出发，沿边serve，找到点204，215 */
=======
| id  |
=======
| 204 |
-------
| 215 |
-------
```


```
nebula> GO FROM 101 OVER serve  \
   WHERE serve.start_year > 1990       /* 筛选边serve的start_year属性  */ \
   YIELD $$.team.name AS team_name,    /* 目标点team的serve.start_year属性 serve.start_year */
================================
| team_name | serve.start_year |
================================
| Spurs     | 1999             |
-------------------------------- 
| Hornets   | 2018             | 
--------------------------------   
```

```
nebula> GO FROM 100,102 OVER serve           \
   WHERE serve.start_year > 1995             /* 筛选边属性*/ \
   YIELD DISTINCT $$.team.name AS team_name, /* DISTINCT与SQL用法相同 */ \
   serve.start_year,                         /* 边属性 */ \
   $^.player.name AS player_name             /* 起点(player)属性 */
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


### 参考
