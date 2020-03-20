# SHOW COLLATION Syntax

```ngql
SHOW COLLATION
```

`SHOW COLLATION` displays the collations supported by **Nebula Graph**. Currently available types are: utf8_bin, utf8_general_ci, utf8mb4_bin and utf8mb4_general_ci. When the character set is utf8, the default collate is utf8_bin; when the character set is utf8mb4, the default collate is utf8mb4_bin. Both utf8_general_ci and utf8mb4_general_ci are case-insensitive comparisons and behave the same as MySQL.

```ngql
nebula> SHOW COLLATION;
=======================
| Collation | Charset |
=======================
| utf8_bin  | utf8    |
-----------------------
```

`SHOW COLLATION` output has these columns:

- Collation
  The collation name.
- Charset
  The name of the character set with which the collation is associated.
