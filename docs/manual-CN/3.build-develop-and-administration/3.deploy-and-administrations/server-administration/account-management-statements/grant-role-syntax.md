# GRANT ROLE 语法

```ngql
GRANT ROLE <role_type> ON <space> TO <user>
```

使用 `GRANT` 语句为 **Nebula Graph** 用户授予权限。使用 `GRANT` 必须拥有 `GRANT` 权限。

目前 **Nebula Graph** 包含五种角色权限：`GOD`、`ADMIN`、`DBA`、`USER` 和 `GUEST`。

通常，需要先使用 `CREATE USER` 创建帐户，然后再使用 `GRANT` 为其授予权限（假设你拥有 `CREATE` 和 `GRANT` 权限）。待授予的角色以及用户帐户必须存在，否则会发生错误。

`<space>` 必须指定为存在的图空间，否则会发生错误。
