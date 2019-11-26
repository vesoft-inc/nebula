# USE 语法

```ngql
USE <graph_space_name>
```

在 **Nebula Graph** 中 `USE` 语句的作用是选择一个图空间来作为当前的工作图空间。USE 需要一些特定的权限来执行。

当前的图空间会保持默认直至当前会话结束或另一个 USE 被执行。

```ngql
nebula> USE space1
nebula> GO FROM 1 OVER edge1   -- 遍历 space1
nebula> USE space2
-- 使用 space2。space2 中的数据与 space1 物理隔离。
nebula> GO FROM 1 OVER edge1
-- 回到 space1。至此你不能从 space2 中读取数据。
nebula> USE space1;
```

和 SQL 不同的是，选取一个当前的工作图空间会阻止你访问其他图空间。遍历一个新的图空间的唯一方案是通过 `USE` 语句来切换工作图空间

> SPACES 之间是 `完全隔离的`。不像 SQL 允许在一个语句中选择两个来自不同数据库的表单，在 Nebula Graph 中一次只能对一个图空间进行操作。
