# Type Conversion

Converting an expression of a given type to another type is known as type-conversion. In nGQL,  type conversion is divided into implicit conversion and explicit conversion.

## Implicit type conversion

Implicit conversions are automatically performed when a value is copied to a compatible type.

1. Following types can implicitly converted to `bool`:

- The conversions from/to bool consider `false` equivalent to `0` for empty string types, true is equivalent to all other values.
- The conversions from/to bool consider `false` equivalent to `0` for int types, true is equivalent to all other values.
- The conversions from/to bool consider `false` equivalent to `0.0` for float types, true is equivalent to all other values.

2. `int` can implicitly converted to `double`.

## Explicit type conversion

In addition to implicit type conversion, explicit type conversion is also supported in case of semantics compliance. The syntax is similar to the `C` language:

`(type_name)expression`.

For example, the results of

`YIELD length((string)123), (int)"123" + 1`

are `3, 124` respectively.

And `YIELD (int)("12ab3")` fails in conversion.
