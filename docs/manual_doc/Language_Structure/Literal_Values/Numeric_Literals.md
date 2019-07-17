Number literals include interger literals and floating-point literals.

Intergers are 64 bit digitals, and can be preceded by + or - to indicate a positive or negative value, respectively. They're the same as `int64_t` in the C language.

floating-points are signed 64 bit digitals. They're `double` in the C language.

Here are some examples:
```
1, -5, +10000100000
-2.3, +1.00000000000
```

Notice that the maximum value for the positive interges is `9223372036854775807`. Any value larger than the maximun will be bounded (no over-flow). So as the minimum value `-9223372036854775808` for the negative interges.
 
Though there are no upper or lower bounds for the doubles. 

Scientific notations will be supported in the next release.
 
