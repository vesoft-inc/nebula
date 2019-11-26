# Data Types

The built-in data types supported by **Nebula Graph** are as follows:

## Numeric Types

### Integer

An integer is declared with keyword `int` , which is 64-bit *signed*, the range is [-9223372036854775808, 9223372036854775807], and there is no overflow in int64-based calculation. Integer constants support multiple formats:

1. Decimal, for example `123456`
1. Hexadecimal, for example `0xdeadbeaf`
1. Octal, for example `01234567`

### Floating Point

Floating point data type is used for storing single precision floating point values. Keyword used for floating point data type is `float`, which is only meaningful for schema definitions and the number of bytes stored. Floating point literal constants are treated as double precision floating point numbers during parsing and arithmetic operations.

### Double Floating Point

Double floating point data type is used for storing double precision floating point values. Keyword used for double floating point data type is `double`. There are no upper and lower ranges.

## Boolean

A boolean data type is declared with the `bool` keyword and can only take the values `true` or `false`.

## String

The string type is used to store a sequence of characters (text). The literal constant is a sequence of characters of any length surrounded by double or single quotes. Line breaks are not allowed in a string. For example `"Shaquile O'Neal"`,`'"This is a double-quoted literal string"'`. Embedding escape sequences are supported within strings, for example:

  1. `"\n\t\r\b\f"`
  1. `"\110ello world"`

## Timestamp

- The supported range of timestamp type is '1970-01-01 00:00:01' UTC to '2262-04-11 23:47:16' UTC
- Timestamp is measured in units of seconds
- Supported data inserting methods
  - call function now()
  - Time string, for example: "2019-10-01 10:00:00"
  - Input the timestamp directly, namely the number of seconds from 1970-01-01 00:00:00
- **Nebula Graph** converts TIMESTAMP values from the current time zone to **UTC** for storage, and back from UTC to the **current time** zone for retrieval

- The underlying storage data type is: **int64**

**Examples**

Create a tag named school

```ngql
nebula> CREATE TAG school(name string , create_time timestamp);
```

Insert a vertex named "stanford" with the foundation date "1885-10-01 08:00:00"

```ngql
nebula> INSERT VERTEX school(name, create_time) VALUES hash("new"):("new", "1985-10-01 08:00:00")
```

Insert a vertex named "dut" with the foundation date now

```ngql
nebula> INSERT VERTEX school(name, create_time) VALUES hash("dut"):("dut", now())
```
