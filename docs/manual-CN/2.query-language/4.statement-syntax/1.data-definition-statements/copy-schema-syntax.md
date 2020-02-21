# COPY SCHEMA FROM 语法

本文档介绍如何复制图空间 schema 而不复制索引。

```ngql
COPY SCHEMA FROM <space_name> [NO INDEX]
```

`COPY SCHEMA FROM` 语句将 schema 从一个图空间复制到另一个空间，复制包含原始图空间中定义的所有默认值但不包括索引（如果使用 NO INDEX 关键字）。

**注意：** 复制 schema 前请先创建并使用一个图空间，然后方可进行复制。语法详情请参考 [CREATE SPACE 语法](create-space-syntax.md)和 [USE 语法](../3.utility-statements/use-syntax.md)。
