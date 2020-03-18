# Built-in Roles

**Nebula Graph** 角色可分为以下几类：

- God
  - 初始用户，与 Linux 系统中的 Root 用户类似。
  - 拥有创建/删除 space 的权限。
  - 拥有读/写 space 中的 schema 和 data 的权限。
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

| Operation | God | Admin | DBA | User | Guest |
| --- | --- | --- | --- | --- | --- |
| **kGo** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kSet** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kPipe** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kUse** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kMatch** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kAssignment** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kCreateTag** | ✅ | ✅ | ✅ |  |  |
| **kAlterTag** | ✅ | ✅ | ✅ |  |  |
| **kCreateEdge** | ✅ | ✅ | ✅ |  |  |
| **kAlterEdge** | ✅ | ✅ | ✅ |  |  |
| **kDescribeTag** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kDescribeEdge** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kCreateTagIndex** | ✅ | ✅ | ✅ |  |  |
| **kCreateEdgeIndex** | ✅ | ✅ | ✅ |  |  |
| **kDropTagIndex** | ✅ | ✅ | ✅ |  |  |
| **kDropEdgeIndex** | ✅ | ✅ | ✅ |  |  |
| **kDescribeTagIndex** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kDescribeEdgeIndex** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kBuildTagIndex** | ✅ | ✅ | ✅ | ✅ |  |
| **kBuildEdgeIndex** | ✅ | ✅ | ✅ | ✅ |  |
| **kDropTag** | ✅ | ✅ | ✅ |  |  |
| **kDropEdge** | ✅ | ✅ | ✅ |  |  |
| **kInsertVertex** | ✅ | ✅ | ✅ | ✅ |  |
| **kUpdateVertex** | ✅ | ✅ | ✅ | ✅ |  |
| **kInsertEdge** | ✅ | ✅ | ✅ | ✅ |  |
| **kUpdateEdge** | ✅ | ✅ | ✅ | ✅ |  |
| **kShow** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kDeleteVertex** | ✅ | ✅ | ✅ | ✅ |  |
| **kDeleteEdges** | ✅ | ✅ | ✅ | ✅ |  |
| **kLookup** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kCreateSpace** | ✅ |  |  |  |  |
| **kDropSpace** | ✅ |  |  |  |  |
| **kDescribeSpace** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kYield** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kCreateUser** | ✅ | ✅ |  |  |  |
| **kDropUser** | ✅ | ✅ |  |  |  |
| **kAlterUser** | ✅ | ✅ |  |  |  |
| **kGrant** | ✅ | ✅ |  |  |  |
| **kRevoke** | ✅ | ✅ |  |  |  |
| **kChangePassword** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kDownload** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kIngest** | ✅ | ✅ | ✅ | ✅ |  |
| **kOrderBy** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kConfig** | ✅ | ✅ | ✅ |  |  |
| **kFetchVertices** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kFetchEdges** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kBalance** | ✅ | ✅ | ✅ |  |  |
| **kFindPath** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kLimit** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **KGroupBy** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kReturn** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **kCreateSnapshot** | ✅ | ✅ | ✅ |  |  |
| **kDropSnapshot** | ✅ | ✅ | ✅ |  |  |
| **kAdmin** | ✅ | ✅ | ✅ |  |  |

按操作权限划分：

- _Read space_: kUse, kDescribeSpace
- _Write space_: kCreateSpace, kDropSpace, kCreateSnapshot, kDropSnapshot,  kBalance, kAdmin, kConfig, kIngest, kDownload
- _Read schema_: kDescribeTag, kDescribeEdge,  kDescribeTagIndex, kDescribeEdgeIndex
- _Write schema_: kCreateTag, kAlterTag, kCreateEdge,  kAlterEdge, kDropTag, kDropEdge, kCreateTagIndex, kCreateEdgeIndex, kDropTagIndex, kDropEdgeIndex,
- _Read user_:
- _Write user_: kCreateUser, kDropUser, kAlterUser, kGrant, kRevoke
- _Read data_: kGo , kSet, kPipe, kMatch, kAssignment, kLookup, kYield, kOrderBy, kFetchVertices, kFind, kFetchEdges, kFindPath, kLimit, KGroupBy, kReturn
- _Write data_: kBuildTagIndex, kBuildEdgeIndex, kInsertVertex, kUpdateVertex, kInsertEdge, kUpdateEdge, kDeleteVertex, kDeleteEdges
- _Special operation_: kShow, kChangePassword

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