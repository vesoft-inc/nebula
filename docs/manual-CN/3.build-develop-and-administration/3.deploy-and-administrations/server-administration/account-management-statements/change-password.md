# CHANGE PASSWORD 语法

```ngql
CHANGE PASSWORD <user_name> FROM <old_psw> TO <new-psw>
```

`CHANGE PASSWORD` 更改 **Nebula Graph** 用户账户密码。更改密码需同时提供新密码和旧密码。为指正账户更改密码需要 `CREATE USER` 权限。为自己更改密码无需特殊权限。
