# Grant Role Syntax

```ngql
GRANT ROLE <role_type> ON <space> TO <user>
```

The `GRANT` statement assigns role to **Nebula Graph** user account. To use `GRANT`, you must have the `GRANT` privilege.

Currently, there are five roles in **Nebula Graph**: `GOD`, `ADMIN`, `DBA`, `USER` and `GUEST`.

Normally, first use `CREATE USER` to create an account then use `GRANT` to define its privileges (assume you have the `CREATE` and `GRANT` privilege). Each role and user account to be granted must exist, or errors will occur.

The `<space>` clause must be specified as an existed graph space or an error will occur.
