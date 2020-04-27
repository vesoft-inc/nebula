# Create User Syntax

```ngql
CREATE USER [IF NOT EXISTS] <user_name> [WITH PASSWORD <password>]
```

The `CREATE USER` statement creates new **Nebula Graph** accounts. To use `CREATE USER`, you must have the global `CREATE USER` privilege. By default, an error occurs if you try to create a user that already exists. If the `IF NOT EXISTS` clause is given, the statement produces a warning for each named user that already exists, rather than an error.
