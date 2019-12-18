# 数据类型

**Nebula Graph** 支持的内建数据类型如下：

## 数值型

### 整型

整型的关键字为 `int`，为 64 位*有符号*整型，范围是[-9223372036854775808, 9223372036854775807]，且在基于 int64 的计算中不存在溢出。整型常量支持多种格式：

  1. 十进制，例如 `123456`
  1. 十六进制，例如 `0xdeadbeaf`
  1. 八进制，例如 `01234567`

### 浮点型

单精度浮点数的关键字为 `float`，且 `float` 仅对 Schema 定义及存储字节数有意义，浮点型字面常量在语法解析以及运算过程中，均被当做双精度浮点数看待。

### 双浮点型

双精度浮点数的关键字为 `double`，且没有上限和下限。

## 布尔型

布尔型关键字为 `bool`，字面常量为 `true` 和 `false`。

## 字符型

字符型关键字为 `string`，字面常量为双引号或单引号包围的任意长度的字符序列，字符串中间不允许换行。例如`"Shaquile O'Neal"`，`'"This is a double-quoted literal string"'`。字符串内支持嵌入转义序列，例如：

  1. `"\n\t\r\b\f"`
  1. `"\110ello world"`

## 时间戳类型

- 时间戳类型的取值范围为 '1970-01-01 00:00:01' UTC  到  '2262-04-11 23:47:16' UTC
- 时间戳单位为秒
- 插入数据的时候，支持插入方式
  - 调用函数 now()
  - 时间字符串，例如："2019-10-01 10:00:00"
  - 直接输入时间戳，即从 1970-01-01 00:00:00 开始的秒数
- 做数据存储的时候，会先将时间转化为 **UTC 时间**，读取的时候会将存储的 **UTC 时间**转换为**本地时间**给用户
- 底层存储数据类型为: **int64**

## 示例

先创建一个名为 school 的 tag

```ngql
nebula> CREATE TAG school(name string , create_time timestamp);
```

插入一个点，名为 "xiwang"，建校时间为 "2010-09-01 08:00:00"

```ngql
nebula> INSERT VERTEX school(name, create_time) VALUES hash("xiwang"):("xiwang", "2010-09-01 08:00:00")
```

插入一个点，名为 "guangming"，建校时间为现在

```ngql
nebula> INSERT VERTEX school(name, create_time) VALUES hash("guangming"):("guangming", now())
```
