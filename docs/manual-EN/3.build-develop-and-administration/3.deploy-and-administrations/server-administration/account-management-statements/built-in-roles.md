# Built-in Roles

**Nebula Graph** provides the following roles:

- God
  - The initial root user similar to the root in Linux.
  - All the operation access.
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

Divided by operation permissions.

- _Read space_: Use, DescribeSpace
- _Write space_: CreateSpace, DropSpace, CreateSnapshot, DropSnapshot, Balance, Admin, Config, Ingest, Download
- _Read schema_: DescribeTag, DescribeEdge,  DescribeTagIndex, DescribeEdgeIndex
- _Write schema_: CreateTag, AlterTag, CreateEdge,  AlterEdge, DropTag, DropEdge, CreateTagIndex, CreateEdgeIndex, DropTagIndex, DropEdgeIndex
- _Read user_:
- _Write user_: CreateUser, DropUser, AlterUser, Grant, Revoke
- _Read data_: Go, Set, Pipe, Match, Assignment, Lookup, Yield, OrderBy, FetchVertices, Find, FetchEdges, FindPath, Limit, GroupBy, Return
- _Write data_: BuildTagIndex, BuildEdgeIndex, InsertVertex, UpdateVertex, InsertEdge, UpdateEdge, DeleteVertex, DeleteEdges
- _Special operation_: Show, ChangePassword

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
