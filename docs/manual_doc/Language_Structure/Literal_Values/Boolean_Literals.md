The boolean literals `TRUE` and `FALSE` can be written in any lettercase.
```
nebula> yield TRUE, true, FALSE, false, FalsE
=========================================
|  true |  true | false | false | false |  
=========================================
```

Notice that `TRUE` and `FALSE` are NOT evaluated to `1` and `0`.
