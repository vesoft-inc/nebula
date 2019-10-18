# Built-in Functions

Nebula Graph supports calling built-in functions of the following types:

## Math

Function| Description |Argument Type| Result
-   |  -|-|-
abs | Return absolute value of the argument | double |double
floor | Return the largest integer value smaller than or equal to the argument. (Rounds down)| double |double
ceil | Return the smallest integer greater than or equal to the argument. (Rounds up) | double |double
round | Return integral value nearest to the argument, returns a number farther away from 0 if the parameter is in the middle| double |double
sqrt | Return the square root of the argument | double|double
cbrt | Return the cubic root of the argument | double|double
hpyot | Return the hypotenuse of a right-angled triangle | double|double
pow | Compute the power of the argument | double|double
exp | Return exponential of the argument | double|double
exp2 | Return 2 raised to the argument | double|double
log | Return natural logarithm of the argument | double|double
log2 | Return the base-2 logarithm of the argument | double|double
log10 | Return the base-10 logarithm of the argument | double|double
sin | Return sine of the argument | double|double
asin | Return inverse sine of the argument| double|double
cos | Return cosine of the argument | double|double
acos | Return inverse cosine of the argument | double|double
tan | Return tangent of the argument | double|double
atan | Return inverse tangent the argument | double|double
rand32 | Return a random 32 bit integer | /| int
rand32(max) | Return a random 32 bit integer in [0, max)  | int| int
rand32(min, max) | Return a random 32 bit integer in [min, max)|int| int
rand64 | Return a random 64 bit integer | /| int
rand64(max) | Return a random 64 bit integer in [0, max) | int| int
rand64( min, max) | Return a random 64 bit integer in [min, max)| int| int

## String


**NOTE:** Like SQL, nGQL's character index (location) starts at `1`, not like C language from `0`.
Function| Description |Argument Type| Result
-   |  -|-|-
strcasecmp(a, b) | Compare strings without case sensitivity, when a = b, return 0, when a > b returned value is greater than 0, otherwise less than 0 | string | int
lower | Return the argument in lowercase | string| string
upper | Return the argument in uppercase | string| string
length | Return length (int) of given string in bytes | string | int
trim | Remove leading and trailing spaces | string | string
ltrim | Remove leading spaces | string| string
rtrim | Remove trailing spaces | string| string
left(string a, int count) | Return the substring in [1, count], if length a is less than count, return a | string, int | string
right(string a, int count) | Return the substring in [size - count + 1, size], if length a is less than count, return a | string, int| string
lpad(string a, int size, string letters) | Left-pads a string with another string to a certain length| string, int| string
rpad(string a, int size, string letters)| Reft-pads a string with another string to a certain length  | string, int| string
substr(string a, int pos, int count) | Extract a substring from a string, starting at the specified position, extract the specified length characters | string, int| string
hash | Encode the data into integer value | string | int

**Explanations** on the returns of function `substr`:

- If pos is 0, return empty string
- If the absolute value of pos is greater than the string, return empty string
- If pos is greater than 0, return substring in [pos, pos + count)
- If pos is less than 0, and set position N as length(a) + pos + 1, return substring in [N, N + count)
- If count is greater than length(a), return the whole string

## Timestamp

Function| Description |Argument Type| Result
-   |  -|-|-
now()  |Return the current date and time (int) | /|int
