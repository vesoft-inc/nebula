# ALTER USER 语法

```ngql
ALTER USER <user_name> WITH PASSWORD <password>
```

使用 `ALTER USER` 语句修改 **Nebula Graph** 帐户。使用 `ALTER USER` 必须拥有全局的 `CREATE USER` 权限。尝试修改一个不存在的用户会发生错误。`ALTER` 无需密码校验。
