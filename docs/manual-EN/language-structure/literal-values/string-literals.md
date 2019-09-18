A string is a sequence of bytes or characters, enclosed within either single quote (') or double quote (") characters. Examples:

```
nebula> YIELD 'a string'
nebula> YIELD "another string"
```

Certain backslash escapes (\\) have been supported (also known as the *escape character*). They are shown in the following table:

| **Escape Sequence**   | **Character Represented by Sequence**   | 
|:----|:----|
| \'   | A single quote (') character   | 
| \"   | A double quote (") character   | 
| \t   | A tab character                | 
| \n   | A newline character            | 
| \b   | A backspace character          | 
| \\  | A backslash (\\) character     | 

Here are some examples:

```
nebula> YIELD 'This\nIs\nFour\nLines'
--------------------
| This
Is
Four
Lines |
--------------------

nebula> YIELD 'disappearing\ backslash'  
--------------------
|   disappearing backslash | 
--------------------


```

