Nebula supports four comment styles:
* From a # character to the end of the line.
* From a --  sequence to the end of the line.
* From a // sequence to the end of the line, as in the C programming language. 
* From a /* sequence to the following */ sequence. This syntax enables a comment to extend over multiple lines because the beginning and closing sequences need not be on the same line.

Nested comments are not supported
The following example demonstrates all these comment styles:

```
(user@127.0.0.1) [(none)]> YIELD 1+1     # This comment continues to the end of line
(user@127.0.0.1) [(none)]> YIELD 1+1     -- This comment continues to the end of line
(user@127.0.0.1) [(none)]> YIELD 1+1     // This comment continues to the end of line
(user@127.0.0.1) [(none)]> YIELD 1 /* this is an in-line comment */ + 1
(user@127.0.0.1) [(none)]> YIELD 11 + \  
/* Multiple-line comment \
Use backslash \
as line break. \
*/ 12
```

