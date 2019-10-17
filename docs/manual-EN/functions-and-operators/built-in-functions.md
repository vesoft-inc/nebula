# Built-in Functions

Nebula Graph supports calling built-in functions of the following types:

## Math

Function| Description |Argument Type
-   |  -|-
abs | Return absolute value of the argument | double
floor | Return floor value of the argument| double
ceil | Return the smallest integer value not less than the argument | double
round | Return integral value nearest to the argument, returns a number farther away from 0 if the parameter is in the middle| double
sqrt | Return the square root of the argument | double
cbrt | Return the cubic root of the argument | double
hpyot | Return the hypotenuse of a right-angled triangle | double
pow | Compute the power of the argument | double
exp | Return exponential of the argument | double
exp2 | Return 2 raised to the argument | double
log | Return natural logarithm of the argument | double
log2 | Return the base-2 logarithm of the argument | double
log10 | Return the base-10 logarithm of the argument | double
sin | Return sine of the argument | double
asin | Return inverse sine of the argument| double
cos | Return cosine of the argument | double
acos | Return inverse cosine of the argument | double
tan | Return tangent of the argument | double
atan | Return inverse tangent the argument | double
rand32 | Return a random 32 bit integer | int
rand32(max) | Return a random 32 bit integer in [0, max)  | int
rand32(min, max) | Return a random 32 bit integer in [min, max)|int
rand64 | Return a random 64 bit integer | int
rand64(max) | Return a random 64 bit integer in [0, max) | int
rand64( min, max) | Return a random 64 bit integer in [min, max)| int

## String


**NOTE:** The index starts from `1` in string.
Function| Description |Argument Type
-   |  -|-
strcasecmp(a, b) | Compare strings without case sensitivity, when a = b, return 0, when a > b returned value is greater than 0, otherwise less than 0 | string
lower | Return the argument in lowercase | string
upper | Return the argument in uppercase | string
length | Return length (int) of given string in bytes | string
trim | Remove leading and trailing spaces | string
ltrim | Remove leading spaces | string
rtrim | Remove trailing spaces | string
left(string a, int count) | Return the substring in [1, count], if length a is less than count, return a | string, int
right(string a, int count) | Return the substring in [size - count + 1, size], if length a is less than count, return a | string, int
lpad(string a, int size, string letters) | Left-pads a string with another string to a certain length| string, int
rpad(string a, int size, string letters)| Reft-pads a string with another string to a certain length  | string, int
substr(string a, int pos, int count) | Extract a substring from a string, starting at the specified position, extract the specified length characters | string, int
hash | Encode the data into integer value | string

**Explanations** on the returns of function `substr`:

- If pos is 0, return empty string
- If the absolute value of pos is greater than the string, return empty string
- If pos is greater than 0, return substring in [pos, pos + count)
- If pos is less than 0, and set position N as length(a) + pos + 1, return substring in [N, N + count)
- If count is greater than length(a), return the whole string

## Timestamp

Function| Description |Argument Type
-   |  -|-
now()  |Return the current date and time (int) | NULL
