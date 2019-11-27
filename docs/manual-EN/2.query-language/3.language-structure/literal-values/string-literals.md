# String Literals

A string is a sequence of bytes or characters, enclosed within either single quote (') or double quote (") characters. Examples:

```ngql
nebula> YIELD 'a string'
nebula> YIELD "another string"
```

Certain backslash escapes (\\) are supported (also known as the *escape characters*). They are shown in the following table:

| **Escape Sequence**   | **Character Represented by Sequence**   |
|:----|:----|
| \'   | A single quote (') character   |
| \"   | A double quote (") character   |
| \t   | A tab character                |
| \n   | A newline character            |
| \b   | A backspace character          |
| \\  | A backslash (\\) character      |

Here are some examples:

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
