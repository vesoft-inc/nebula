# USE

```
USE graph_space_name
```

在 Nebula 中 `USE` 语句的作用是选择一个图空间来作为当前的工作图空间。USE 需要一些特定的权限来执行。

当前的图空间会保持默认直至当前会话结束或另一个 USE 被执行。

```
USE space1
GO FROM 1 OVER edge1   -- 遍历 space1
USE space2
GO FROM 1 OVER edge1   -- 遍历 space2。所有的 nodes 和 edges 与 space1 无关.
USE space1;            -- 此时你回到了 space1。 至此你不能从 space2 中读取数据。
```

和 SQL 不同的是，选取一个当前的工作图空间会阻止你访问其他图空间。遍历一个新的图空间的唯一方案是通过 `USE` 语句来切换工作图空间

> SPACES 之间是 `完全隔离的`。不像 SQL 允许在一个语句中选择两个来自不同数据库的表单，在 Nebula 中一时间只能对一个图空间进行操作。

