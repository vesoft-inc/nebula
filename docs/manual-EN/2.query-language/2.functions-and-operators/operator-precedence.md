# Operator Precedence

The following list shows the precedence of nGQL operators in descending order. Operators that are shown together on a line have the same precedence.

```ngql
!
- (unary minus)
*, /, %
-, +
== , >=, >, <=, <, <>, !=
&&
||
= (assignment)
```

For operators that occur at the same precedence level within an expression, evaluation proceeds left to right, with the exception that assignments evaluate right to left.

The precedence of operators determines the order of evaluation of terms in an expression. To override this order and group terms explicitly, use parentheses.

Examples:

```ngql
nebula> YIELD 2+3*5;
nebula> YIELD (2+3)*5;
```
