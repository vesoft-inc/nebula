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

If the authorization is enabled, the default user name and password are `root` and `nebula` respectively, and the user name is immutable. Set the `enable_authorize` parameter in the `/usr/local/nebula/etc/nebula-graphd.conf` file to `true` to enable the authorization.

A user who has no assigned roles will not have any accesses to the space. A user can only have one assigned role in the same space. A user can have different roles in different spaces.

The set of executor prescribed by each role are described below.

Divided by operation permissions.

| OPERATION | STATEMENTS |
| --- | --- |
| Read space | Use, DescribeSpace |
| Write space | CreateSpace, DropSpace, CreateSnapshot, DropSnapshot, Balance, Admin, Config, Ingest, Download |
| Read schema |  DescribeTag, DescribeEdge,  DescribeTagIndex, DescribeEdgeIndex |
| Write schema | CreateTag, AlterTag, CreateEdge,  AlterEdge, DropTag, DropEdge, CreateTagIndex, CreateEdgeIndex, DropTagIndex, DropEdgeIndex |
| Write user | CreateUser, DropUser, AlterUser |
| Write role | Grant, Revoke |
| Read data | Go, Set, Pipe, Match, Assignment, Lookup, Yield, OrderBy, FetchVertices, Find, FetchEdges, FindPath, Limit, GroupBy, Return |
| Write data | BuildTagIndex, BuildEdgeIndex, InsertVertex, UpdateVertex, InsertEdge, UpdateEdge, DeleteVertex, DeleteEdges |
| Special operation | Show, ChangePassword |

Divided by operations.

| OPERATION | GOD | ADMIN | DBA | USER | GUEST |
| --- | --- | --- | --- | --- | --- |
| Read space | Y | Y | Y | Y | Y |
| Write space | Y |  |  |  |  |
| Read schema | Y | Y | Y | Y | Y |
| Write schema | Y | Y | Y |  |  |
| Write user | Y |  |  |  |  |
| Write role | Y | Y |  |  |  |
| Read data | Y | Y | Y | Y | Y |
| Write data | Y | Y | Y | Y |  |
| Special operation | Y | Y | Y | Y | Y |
