<!---

>User-Defined Variable can only be used in one execution ( compound statements separated by semicolon (;) or pipe (|) and be submitted to server to execute together)
You can temporally store a value in a user-defined variable in the previous statement and use it later in another statement. This enables you to pass values from one statement to another.

User variables are written as `$*var_name*`, where the variable name `*var_name*` consists of alphanumeric character, ., _,. Currently, any other characters are not recommend.

Be aware that user-defined variables are session and execution specific. A user variable defined in one statement can NOT be used in neither other clients nor other executions, which means that the definition statement and the statements that use it should be submitted together. Variables are automatically freed when client has send the executions.

User variable names are case-sensitive. Names are recommended to have a maximum length of 256 characters.

One way to use a user-defined variable is by the YIELD statement:
```
YIELD $var1 = expr [, $var2 = expr]
```
-->
