# 字符串字面值

字符串由一串字节或字符组成，并由一对单引号 (') 或双引号 (") 包装。

```ngql
nebula> YIELD 'a string'
nebula> YIELD "another string"
```

一些转义字符 (\\) 已被支持，如下表所示:

| **转义字符**   | **对应的字符**   |
|:----|:----|
| \'   | 单引号 (')  |
| \"   | 双引号 (")  |
| \t   | 制表符      |
| \n   | 换行符      |
| \b   | 退格符      |
| \\  | 反斜杠 (\\) |

示例:

```ngql
nebula> YIELD 'This\nIs\nFour\nLines'
========================
| "This
Is
Four
Lines" |
========================
| This
Is
Four
Lines   |
------------------------

nebula> YIELD 'disappearing\ backslash'  
============================
| "disappearing backslash" |
============================
| disappearing backslash   |
----------------------------
```
