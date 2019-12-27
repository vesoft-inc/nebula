# User-Defined Variables

**Nebula Graph** supports user-defined variables, which allows passing the result of one statement to another. A user-defined variable is written as `$var_name`, where `var_name` is a user-defined name/variable that consists of alphanumeric characters, any other characters are not recommended currently.

User-defined variables can only be used in one execution (compound statements separated by semicolon `;` or pipe `|` and are submitted to the server to execute together).

Be noted that a user-defined variable is valid only at the current session and execution. A user-defined variable in one statement can NOT be used in neither other clients nor other executions, which means that the definition statement and the statements that use it must be submitted together. And when the session ends these variables are automatically expired.

User-defined variables are case-sensitive.

Example:

```ngql
nebula> $var = GO FROM hash('curry') OVER follow YIELD follow._dst AS id; \
GO FROM $var.id OVER serve;
```
