# SHOW COLLATION 语法

```ngql
SHOW COLLATION
```

`SHOW COLLATION` 语句列出 **Nebula Graph** 目前支持的所有排序规则。目前支持四种排序规则：utf8_bin、utf8_general_ci、utf8mb4_bin、utf8mb4_general_ci。字符集为 utf8 时，默认 collate 为 utf8_bin；字符集为 utf8mb4 时，默认 collate 为 utf8mb4_bin。utf8_general_ci 和 utf8mb4_general_ci 都是忽略大小写的比较，行为同 MySQL 一致。

```ngql
nebula> SHOW COLLATION;
=======================
| Collation | Charset |
=======================
| utf8_bin  | utf8    |
-----------------------
```

`SHOW COLLATION` 输出有以下列：

- Collation
  排序规则名称。
- Charset
  与排序规则关联的字符集的名称。
