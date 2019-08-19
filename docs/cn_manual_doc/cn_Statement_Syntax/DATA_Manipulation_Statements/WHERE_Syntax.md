目前，`WHERE`语句仅适用于`GO`语句。

```
WHERE (expression [ AND | OR expression ...])  
```

通常，筛选条件是关于节点、边的表达式的逻辑组合。

>作为语法糖，逻辑和可用`AND`或`&&`，同理，逻辑或可用`OR`或`||`表示。

### 示例

```
/* GO FROM 201 OVER like */  -- 适用于GO语句
WHERE e1.prop1 >= 17     -- 边e1的prop1属性大于17

WHERE $^.v1.prop1 == $$.v2.prop2  -- 起点v1的prop1属性与终点v2的prop2属性值相等

WHERE ((e3.prop3 < 0.5) OR ($^.v4.prop4 != "hello")) AND $$.v5.prop5 == "world"   -- logical combination is allowed

WHERE 1 == 1 OR TRUE    --always TRUE
```

### 参考
