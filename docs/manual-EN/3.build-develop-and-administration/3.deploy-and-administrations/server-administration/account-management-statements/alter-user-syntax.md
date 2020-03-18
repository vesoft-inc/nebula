# Alter User Syntax

```ngql
ALTER USER <user_name> WITH PASSWORD <password>
```

The `ALTER USER` statement modifies **Nebula Graph** user accounts. `ALTER USER` requires the global `CREATE USER` privilege. An error occurs if you try to modify a user that does not exist. `ALTER` does not require password verification.
