# CREATE USER 语法

```ngql
CREATE USER [IF NOT EXISTS] <user_name> [WITH PASSWORD <password>]
```

使用 `CREATE USER` 语句创建新的 **Nebula Graph** 帐户。使用 `CREATE USER` 必须拥有全局的 `CREATE USER` 权限。默认情况下，尝试创建一个已经存在的用户会发生错误。如果使用 `IF NOT EXISTS` 子句，则会提示用户名已存在。
