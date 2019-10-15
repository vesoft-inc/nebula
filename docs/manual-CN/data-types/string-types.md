# String Data Types
String 是动态长度字符串，没有最长长度限制。nebula 支持使用 `'` 和`"`表示字符串。
字符串数据类型及介绍见下表：

| Data Type | Description |
| ----- | ---- | 
| CHAR(size) | 保存固定长度的字符串（可包含字母、数字以及特殊字符）。在括号中指定字符串的长度。最多 255 个字符。 | 
| VARCHAR(size) |保存可变长度的字符串（可包含字母、数字以及特殊字符）。在括号中指定字符串的长度。最多 65535 字节的数据。 | 
| TINYTEXT | 存放最大长度为 255 个字符的字符串。 |
| TEXT(size)	 | H存放最大长度为 65,535 字节的数据。 | 
| BLOB(size) | 用于 BLOBs (Binary Large OBjects)。存放最多 65,535 字节的数据。| 
| MEDIUMTEXT | 存放最大长度为 16,777,215 个字符的字符串。| 
| MEDIUMBLOB | 用于 BLOBs (Binary Large OBjects)。存放最多 16,777,215 字节的数据。 | 
| LONGTEXT | 存放最大长度为 4,294,967,295 个字符的字符串。 | 
| LONGBLOB | 用于 BLOBs (Binary Large OBjects)。存放最多 4,294,967,295 字节的数据。 | 
| ENUM(val1, val2, val3, ...) |允许你输入可能值的列表。可以在 ENUM 列表中列出最大 65535 个值。如果列表中不存在插入的值，则插入空值。这些值是按照你输入的顺序存储的。 | 
| SET(val1, val2, val3, ...) |与 ENUM 类似，SET 最多只能包含 64 个列表项，不过 SET 可存储一个以上的值。| 
