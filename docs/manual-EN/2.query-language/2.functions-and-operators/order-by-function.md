# Order By Function

Similar with SQL, `ORDER BY` can be used to sort in ascending (`ASC`) or descending (`DESC`) order for return results.
And it can only be used in the `PIPE`-syntax ("|")

```ngql
| ORDER BY <prop> ASC | DESC [, <prop> ASC | DESC ...] 
```

By default, `ORDER BY` sorts the records in ascending order if no `ASC` or `DESC` is given.

## Example

```ngql
nebula> FETCH PROP ON player 1,2,3,4 YIELD player.age AS age, player.weight AS weight | ORDER BY $-.age, $-.weight DESC  
-- get four of vertices and sort by their age begin with the youngest one, and for those with the same age, sort by their weight.
```

(see `FETCH` for the usage)

```ngql
nebula> GO FROM 1 OVER edge2 YIELD $^.t1.prop1 AS s1_p1, edge2.prop2 AS e2_p2, $$.t3.prop3 AS d3_p3 | ORDER BY s1_p1 ASC, e2_p2 DESC, d3_p3 ASC
```

For a group of returned tuples <s1_p1, e2_p2, d3_p3>, first sort in ascending order of s1_p1, then in descending order of e2_p2, finally ascending order of d3_p3.
