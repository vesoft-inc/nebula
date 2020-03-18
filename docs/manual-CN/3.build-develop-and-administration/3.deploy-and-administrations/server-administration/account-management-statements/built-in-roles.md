# Built-in Roles

**Nebula Graph** 角色可分为以下几类：

- God
  - 初始 Root 用户，与 Linux 系统中的 Root 用户类似。
  - 拥有所有操作权限。
- Admin
  - 管理员用户。
  - 对权限内的 space 拥有 schema 和 data 的读/写权限。
  - 可对权限内的 space 进行用户受权。
- DBA
  - 对权限内的 space 拥有 schema 和 data 的读/写权限。
  - 没有对用户受权的权限。
- User
  - 对权限内的 space 拥有 data 的读/写权限。
  - 对权限内的 space 拥有 schema 只读权限。
- Guest
  - 对权限内的 space 拥有 schema 和 data 的只读权限。

未被分配角色的用户将无权访问该 space。一个用户只能分配一个角色。一个用户在不同 space 可拥有不同权限。

各角色的 Executor 权限见下表。

按操作权限划分。

- _Read space_: Use, DescribeSpace
- _Write space_: CreateSpace, DropSpace, CreateSnapshot, DropSnapshot, Balance, Admin, Config, Ingest, Download
- _Read schema_: DescribeTag, DescribeEdge,  DescribeTagIndex, DescribeEdgeIndex
- _Write schema_: CreateTag, AlterTag, CreateEdge,  AlterEdge, DropTag, DropEdge, CreateTagIndex, CreateEdgeIndex, DropTagIndex, DropEdgeIndex
- _Read user_:
- _Write user_: CreateUser, DropUser, AlterUser, Grant, Revoke
- _Read data_: Go, Set, Pipe, Match, Assignment, Lookup, Yield, OrderBy, FetchVertices, Find, FetchEdges, FindPath, Limit, GroupBy, Return
- _Write data_: BuildTagIndex, BuildEdgeIndex, InsertVertex, UpdateVertex, InsertEdge, UpdateEdge, DeleteVertex, DeleteEdges
- _Special operation_: Show, ChangePassword

按操作划分。

| OP | GOD | ADMIN | DBA | USER | GUEST |
| --- | --- | --- | --- | --- | --- |
| Read space | ✅ | ✅ | ✅ |  |  |
| Write space | ✅ |  |  |  |  |
| Read schema | ✅ | ✅ | ✅ |  |  |
| Write schema | ✅ | ✅ |  |  |  |
| Read user | ✅ | ✅ | ✅ |  |  |
| Write user | ✅ | ✅ |  |  |  |
| Read data | ✅ | ✅ | ✅ | ✅ | ✅ |
| Write data | ✅ | ✅ | ✅ | ✅ |  |
| Special operation | ✅ | ✅ | ✅ | ✅ | ✅ |
