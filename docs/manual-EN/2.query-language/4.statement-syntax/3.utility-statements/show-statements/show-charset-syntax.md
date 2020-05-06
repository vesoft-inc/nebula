# SHOW CHARSET Syntax

```ngql
SHOW CHARSET
```

`SHOW CHARSET` displays the available character sets. Currently available types are: utf8 and utf8mb4. The default charset type is utf8. **Nebula Graph** extends the uft8 to support four byte characters. Therefore utf8 and utf8mb4 equivalent.

```ngql
nebula> SHOW CHARSET;
========================================================
| Charset | Description   | Default collation | Maxlen |
========================================================
| utf8    | UTF-8 Unicode | utf8_bin          | 4      |
--------------------------------------------------------
```

`SHOW CHARSET` output has these columns:

- Charset
  The character set name.
- Description
  A description of the character set.
- Default collation
  The default collation for the character set.
- Maxlen
  The maximum number of bytes required to store one character.
