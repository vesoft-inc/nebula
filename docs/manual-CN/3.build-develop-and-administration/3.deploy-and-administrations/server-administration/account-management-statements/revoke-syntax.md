# REVOKE 语法

```ngql
REVOKE ROLE <role_type> ON <space> FROM <user>
```

使用 `REVOKE` 语句从 **Nebula Graph** 用户删除权限。使用 `REVOKE` 必须拥有 `REVOKE` 权限。

目前 **Nebula Graph** 包含五种角色权限：`GOD`、`ADMIN`、`DBA`、`USER` 和 `GUEST`。

待删除的角色以及用户帐户必须存在，否则会发生错误。

`<space>` 必须指定为存在的图空间，否则会发生错误。
