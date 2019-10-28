# WHERE 语法

目前，`WHERE`语法仅适用于`GO`语法。

```sql
WHERE (expression [ AND | OR expression ...])  
```

通常，筛选条件是关于节点、边的表达式的逻辑组合。

> 作为语法糖，逻辑和可用`AND`或`&&`，同理，逻辑或可用`OR`或`||`表示。

## 示例

```SQL
-- 边e1的prop1属性大于17
nebula> GO FROM 201 OVER likes WHERE e1.prop1 >= 17
-- 起点v1的prop1属性与终点v2的prop2属性值相等
nebula> GO FROM 201 OVER likes WHERE $^.v1.prop1 == $$.v2.prop2
-- 多种逻辑组合logical combination is allowed
nebula> GO FROM 201 OVER likes WHERE ((e3.prop3 < 0.5) \ 
   OR ($^.v4.prop4 != "hello")) AND $$.v5.prop5 == "world"
--下面这个条件总是为TRUE
nebula> GO FROM 201 OVER likes WHERE 1 == 1 OR TRUE
```
