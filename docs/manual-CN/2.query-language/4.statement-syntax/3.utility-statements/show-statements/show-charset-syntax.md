# SHOW CHARSET 语法

```ngql
SHOW CHARSET
```

`SHOW CHARSET` 返回所有可用的字符集。目前支持两种类型：utf8、utf8mb4。其中默认字符集为 utf8。**Nebula Graph** 将 utf8 进行了扩展，utf8 同时支持 4 个字节的字符，因此，utf8 和 utf8mb4 是等价的。

```ngql
nebula> SHOW CHARSET;
========================================================
| Charset | Description   | Default collation | Maxlen |
========================================================
| utf8    | UTF-8 Unicode | utf8_bin          | 4      |
--------------------------------------------------------
```

`SHOW CHARSET` 输出以下列：

- Charset
  字符集名称。
- Description
  字符集的描述。
- Default collation
  字符集的默认排序规则。
- Maxlen
  存储一个字符所需的最大字节数。
