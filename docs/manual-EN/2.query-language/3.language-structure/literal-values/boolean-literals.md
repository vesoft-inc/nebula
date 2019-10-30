# Boolean Literals

The boolean literals `TRUE` and `FALSE` can be written in any letter case.

```sql
nebula> yield TRUE, true, FALSE, false, FalsE
=========================================
| true  | true  | false | false | false |
=========================================
| true  | true  | false | false | false |
-----------------------------------------
```
