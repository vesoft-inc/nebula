# Update Syntax

**Nebula Graph** supports `UPDATE` properties of a vertex or an edge, as well as CAS operation and returning related properties.

## Update Vertex

```ngql
UPDATE VERTEX <vid> SET <update_columns> WHEN <condition> YIELD <columns>
```

**NOTE:** `WHEN` and `YIELD` are optional.

- `vid` is the id of the vertex to be updated.
- `update_columns` is the properties of the vertex to be updated, for example, `tag1.col1 = $^.tag2.col2 + 1` means to update `tag1.col1` to `tag2.col2+1`.

    **NOTE:**  `$^` indicates vertex to be updated
- `condition` is some constraints, only when met, `UPDATE` will run successfully and expression operations are supported.
- `columns` is the columns to be returned, `YIELD` returns the latest updated values.

Consider the following example:

```ngql
nebula> UPDATE VERTEX 101 SET course.credits = $^.course.credits + 1, building.name = "No8" WHEN $^.course.name == "Math" && $^.course.credits > 2 YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name
```

There are two tags in vertex 101, namely course and building.

## Update Edge

```ngql
UPDATE EDGE <edge> SET <update_columns> WHEN <condition> YIELD <columns>
```

**NOTE:** `WHEN` and `YIELD` are optional.

- `edge` is the edge to be updated, the syntax is `$src->$dst@$rank OF $type`.
- `update_columns` is the properties of the edge to be updated.
- `condition` is some constraints, only when met, `UPDATE` will run successfully and expression operations are supported.
- `columns` is the columns to be returned, `YIELD` returns the latest updated values.

Consider the following example:

```ngql
nebula> UPDATE EDGE 200 -> 101@0 OF select SET grade = select.grade + 1, year = 2000 WHEN select.grade > 4 && $^.student.age > 15 YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
```

> **NOTE:** The constraints are the source student's age, the returns are properties of the vertex and edge.
