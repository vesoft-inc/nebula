# Built-in Roles

**Nebula Graph** provides the following roles:

- God
  - The initial user similar to the root in Linux.
  - Create/delete access to spaces.
  - Read/write access to both the schema and data in the space.
- Admin
  - The administration user.
  - Read/write access to both the schema and data limited to its authorized space.
  - Authorization access to users limited to its authorized space.
- DBA
  - Read/write access to both the schema and data limited to its authorized space.
  - No authorization access to users.
- User
  - Read/write access to data limited to its authorized space.
  - Read-only access to the schema limited to its authorized space.
- Guest
  - Read-only access to both the schema and data limited to its authorized space.

A user who has no assigned roles will not have any accesses to the space. A user can only have one assigned role. A user can have different roles in different spaces.

The set of executor prescribed by each role are described below.

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

Divided by operation permissions.

- _Read space_: kUse, kDescribeSpace
- _Write space_: kCreateSpace, kDropSpace, kCreateSnapshot, kDropSnapshot,  kBalance, kAdmin, kConfig, kIngest, kDownload
- _Read schema_: kDescribeTag, kDescribeEdge,  kDescribeTagIndex, kDescribeEdgeIndex
- _Write schema_: kCreateTag, kAlterTag, kCreateEdge,  kAlterEdge, kDropTag, kDropEdge, kCreateTagIndex, kCreateEdgeIndex, kDropTagIndex, kDropEdgeIndex,
- _Read user_:
- _Write user_: kCreateUser, kDropUser, kAlterUser, kGrant, kRevoke
- _Read data_: kGo , kSet, kPipe, kMatch, kAssignment, kLookup, kYield, kOrderBy, kFetchVertices, kFind, kFetchEdges, kFindPath, kLimit, KGroupBy, kReturn
- _Write data_: kBuildTagIndex, kBuildEdgeIndex, kInsertVertex, kUpdateVertex, kInsertEdge, kUpdateEdge, kDeleteVertex, kDeleteEdges
- _Special operation_: kShow, kChangePassword

Divided by operations.

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
