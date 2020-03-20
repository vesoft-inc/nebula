# Grant Role Syntax

```ngql
GRANT ROLE <role_type> ON <space> TO <user>
```

The `GRANT` statement assigns roles to **Nebula Graph** user accounts. To use `GRANT`, you must have the `GRANT` privilege.

Currently, there are four roles in **Nebula Graph**: `GOD`, `ADMIN`, `DBA` and `GUEST`.

Normally, a database administrator first uses `CREATE USER` to create an account then uses `GRANT` to define its privileges. Each role and user account to be granted must exist, or errors will occur.

The `<space>` clause must be specified as an existed graph space or an error will occur.
