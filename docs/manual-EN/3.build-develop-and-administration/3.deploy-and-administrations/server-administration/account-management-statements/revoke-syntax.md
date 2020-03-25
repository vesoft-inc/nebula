# Revoke Syntax

```ngql
REVOKE ROLE <role_type> ON <space> FROM <user>
```

The `REVOKE` statement removes access privileges from **Nebula Graph** user accounts. To use `REVOKE`, you must have the `REVOKE` privilege.

Currently, there are five roles in **Nebula Graph**: `GOD`, `ADMIN`, `DBA`, `USER` and `GUEST`.

<!-- For information about roles, see [Using Roles](TODO). -->

User accounts and roles are to be revoked must exist, or errors will occur.

The `<space>` clause must be specified as an existed graph space or an error will occur.
