# Operator Precedence

The following list shows the precedence of nGQL operators, in descending order. Operators on a line have the same precedence.

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

For operators from the same precedence level within an expression, evaluation is from left to right, with the exception that assignment evaluates right to left. However, parentheses can be used to modify the order.

Examples:

```ngql
nebula> YIELD 2+3*5;
nebula> YIELD (2+3)*5;
```
