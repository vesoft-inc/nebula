# User-Defined Variables

**Nebula Graph** supports the concept of user-defined variables, which allows passing of a value from one statement to another. A user-defined variable in written as `$ * var_name *`, var_name is the name of variable and can consist of alphanumeric characters, any other characters are not recommended currently.

User-defined variables can only be used in one execution (compound statements separated by semicolon (;) or pipe (|) and be submitted to server to execute together).

Be noted that user-defined variables are session and execution specific. A user-defined variable in one statement can NOT be used in neither other clients nor other executions, which means that the definition statement and the statements that use it should be submitted together. And when the session ends these variables are automatically expired.

User-defined variables are case-sensitive. The recommended maximum length of variables can be 256 characters.

One way to use a user-defined variable is by the YIELD statement:

```ngql
YIELD $var1 = expr [, $var2 = expr]
```
