# UPDATE 语法

**Nebula Graph** 支持 `UPDATE` 一个点或者一条边的属性，支持 CAS 操作，支持返回相关的属性。

## 更新点

```ngql
UPDATE VERTEX <vid> SET <update_columns> WHEN <condition> YIELD <columns>
```

**注意：**`WHEN` 和 `YIELD` 是可选的。

- `vid` 表示需要更新的 vertex ID。
- `update_columns` 表示需要更新的 tag 上的 columns，比如 `tag1.col1 = $^.tag2.col2 + 1` 表示把这个点的 `tag1.col1` 更新成 `tag2.col2 + 1`。

    **注意：**  `$^`表示 `UPDATE` 中需要更新的点

- `condition` 是一些约束条件，只有满足这个条件，`UPDATE` 才会真正执行，支持表达式操作。
- `columns` 表示需要返回的 columns，此处 `YIELD` 可返回 update 以后最新的 columns 值。

### 示例

```ngql
nebula> UPDATE VERTEX 101 SET course.credits = $^.course.credits + 1, building.name = "No8" WHEN $^.course.name == "Math" && $^.course.credits > 2 YIELD $^.course.name AS Name, $^.course.credits AS Credits, $^.building.name
```

这个例子里面，101 共有两个 tag，即 course 和 building。

## 更新边

```ngql
UPDATE EDGE <edge> SET <update_columns> WHEN <condition> YIELD <columns>
```

**注意：**`WHEN` 和 `YIELD` 是可选的。

- `edge` 表示需要更新的 edge，edge 的格式为 `$src->$dst@$rank OF $type`。
- `update_columns` 表示需要更新的 edge 上的属性。
- `condition` 是一些约束条件，只有满足这个条件，update 才会真正执行，支持表达式操作。
- `columns` 表示需要返回的 columns，此处 YIELD 可返回 update 以后最新的 columns 值。

### 示例

```ngql
nebula> UPDATE EDGE 200 -> 101@0 OF select SET grade = select.grade + 1, \
  year = 2000 WHEN select.grade > 4 && $^.student.age > 15 \
  YIELD $^.student.name AS Name, select.grade AS Grade, select.year AS Year
```

**注意**：本例中 `WHEN` 后面的约束条件为起点 student 这个 tag 的 age 属性，同时也返回了点和边的属性。
