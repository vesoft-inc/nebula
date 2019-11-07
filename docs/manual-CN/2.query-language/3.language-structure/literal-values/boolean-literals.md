# 布尔字面值

布尔字面值 `TRUE` 和 `FALSE` 对大小写不敏感。

```ngql
nebula> yield TRUE, true, FALSE, false, FalsE
=========================================
| true  | true  | false | false | false |
=========================================
| true  | true  | false | false | false |
-----------------------------------------
```
