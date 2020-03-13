# GRANT ROLE 语法

```ngql
REVOKE ROLE <role_type> ON <space> FROM <user>
```

使用 `GRANT` 语句为 **Nebula Graph** 用户授予权限。使用 `GRANT` 必须拥有 `GRANT` 权限。

目前 **Nebula Graph** 包含四种角色权限：`GOD`、`ADMIN`、`DBA` 和 `GUEST`。

通常，需要先使用 `CREATE USER` 创建帐户，然后再使用 `GRANT` 为其授予权限。待授予的角色以及用户帐户必须存在，否则会发生错误。

`<space>` 必须指定为存在的图空间，否则会发生错误。
