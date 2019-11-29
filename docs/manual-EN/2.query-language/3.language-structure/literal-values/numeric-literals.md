# Numeric Literals

Numeric literals include integers literals and floating-point literals (doubles).

## Integers Literals

Integers are 64 bit digitals, and can be preceded by + or - to indicate a positive or negative value, respectively. They're the same as `int64_t` in the C language.

Notice that the maximum value for the positive integers is `9223372036854775807`. It's syntax-error if you try to input any value greater than the maximum. So as the minimum value `-9223372036854775808` for the negative integers.

## Floating-Point Literals (Doubles)

Floating-points are the same as `double` in the `C` language.

The range for double is about `-1.79769e+308` to `1.79769e+308`.

Scientific notations is not supported yet.

## Examples

Here are some examples:

```ngql
1, -5, +10000100000
-2.3, +1.00000000000
```
