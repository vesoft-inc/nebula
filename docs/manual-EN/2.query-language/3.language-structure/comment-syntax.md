# Comment Syntax

**Nebula Graph** supports four comment styles:

* From a # character to the end of the line.
* From a --  sequence to the end of the line.
* From a // sequence to the end of the line, as in the C programming language.
* From a /* sequence to the following */ sequence. This syntax enables a comment to extend over multiple lines because the beginning and closing sequences need not be on the same line.

Nested comments are not supported.
The following example demonstrates all these comment styles:

```ngql
nebula> -- Do nothing this line
nebula> YIELD 1+1     # This comment continues to the end of line
nebula> YIELD 1+1     -- This comment continues to the end of line
nebula> YIELD 1+1     // This comment continues to the end of line
nebula> YIELD 1       /* This is an in-line comment */ + 1
nebula> YIELD 11 +             \  
/* Multiple-line comment       \
Use backslash as line break.   \
*/ 12
```

The backslash `\` in a line indicates a line break.
