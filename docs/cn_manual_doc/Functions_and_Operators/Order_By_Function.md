# Order By 函数

类似于 SQL, `ORDER BY` 可以进行升序 (`ASC`) 或降序 (`DESC`) 的排序来返回结果.
并且他只能在`PIEP`-语法 ("|") 下使用

```
| ORDER BY <prop> ASC | DESC [, <prop> ASC | DESC ...] 
```
如果没有知名ASC 或 DESC，`ORDER BY` 将默认进行升序排序。 

### 示例

```
nebula> FETCH PROP ON player 1,2,3,4 YIELD player.age AS age, player.weight as weight | ORDER BY $-.age, $-.weight DESC  

-- 取4个顶点并将他们以age从小到大的顺序排列，如age一致，择按weight从小到大的顺序排列。
```
(参见 `FETCH` 文档来了解使用方法)

```
nebula> GO FROM 1 OVER edge2 YIELD $^.t1.prop1 AS s1_p1, edge2.prop2 AS e2_p2, $$.t3.prop3 AS d3_p3 | ORDER BY s1_p1 ASC, e2_p2 DESC, d3_p3 ASC

-- For a group of returned tuples <s1_p1, e2_p2, d3_p3>, first sort in ascending order of s1_p1, then in descending order of e2_p2, finally ascending order of d3_p3.
```
