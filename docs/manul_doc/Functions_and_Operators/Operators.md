| Name   | Description   | 
|:----|:----|
| &&   | Logical AND   | 
| =   | Assign a value   | 
| \|\|   | Logical OR   | 
| /   | Division operator   | 
| ==   | Equal operator   | 
| !=   | Not equal operator   | 
| <   | Less than operator   | 
| <=   | Less than or equal operator   | 
| -   | Minus operator   | 
| %   | Modulo operator   | 
| +   | Addition operator   | 
| !   | Logical NOT   | 
| *   | Multiplication operator   | 
| -   | Change the sign of the argument   | 


Example:

```
nebula> $a=GO FROM 201 OVER like; GO FROM $a.id OVER select YIELD $^.student.name;
```