# Built-in Functions

Nebula Graph supports calling built-in functions of the following types:

## Math

Function| Description |
----  |  ----|
double abs(double x) | Return absolute value of the argument |
double floor(double x) | Return the largest integer value smaller than or equal to the argument. (Rounds down)|
double ceil(double x) | Return the smallest integer greater than or equal to the argument. (Rounds up) |
double round(double x) | Return integral value nearest to the argument, returns a number farther away from 0 if the parameter is in the middle|
double sqrt(double x) | Return the square root of the argument |
double cbrt(double x) | Return the cubic root of the argument |
double hypot(double x, double x) | Return the hypotenuse of a right-angled triangle |
double pow(double x, double y) | Compute the power of the argument |
double exp(double x) | Return the value of e raised to the x power |
double exp2(double x) | Return 2 raised to the argument |
double log(double x) | Return natural logarithm of the argument |
double log2(double x) | Return the base-2 logarithm of the argument |
double log10(double x) | Return the base-10 logarithm of the argument |
double sin(double x) | Return sine of the argument |
double asin(double x) | Return inverse sine of the argument|
double cos(double x) | Return cosine of the argument |
double acos(double x) | Return inverse cosine of the argument |
double tan(double x) | Return tangent of the argument |
double atan(double x) | Return inverse tangent the argument |
int rand32() | Return a random 32 bit integer |
int rand32(int max) | Return a random 32 bit integer in [0, max)  |
int rand32(int min, int max) | Return a random 32 bit integer in [min, max)|
int rand64() | Return a random 64 bit integer |
int rand64(int max) | Return a random 64 bit integer in [0, max) |
int rand64(int min, int max) | Return a random 64 bit integer in [min, max)|

## String

**NOTE:** Like SQL, nGQL's character index (location) starts at `1`, not like C language from `0`.

Function| Description |
----  |  ----|
int strcasecmp(string a, string b) | Compare strings without case sensitivity, when a = b, return 0, when a > b returned value is greater than 0, otherwise less than 0 |
string lower(string a) | Return the argument in lowercase |
string upper(string a) | Return the argument in uppercase |
int length(string a) | Return length (int) of given string in bytes |
string trim(string a) | Remove leading and trailing spaces |
string ltrim(string a) | Remove leading spaces |
string rtrim(string a) | Remove trailing spaces |
string left(string a, int count) | Return the substring in [1, count], if length a is less than count, return a |
string right(string a, int count) | Return the substring in [size - count + 1, size], if length a is less than count, return a |
string lpad(string a, int size, string letters) | Left-pads a string with another string to a certain length|
string rpad(string a, int size, string letters)| Reft-pads a string with another string to a certain length  |
string substr(string a, int pos, int count) | Extract a substring from a string, starting at the specified position, extract the specified length characters |
int hash(string a) | Encode the data into integer value |

**Explanations** on the returns of function `substr`:

- If pos is 0, return empty string
- If the absolute value of pos is greater than the string, return empty string
- If pos is greater than 0, return substring in [pos, pos + count)
- If pos is less than 0, and set position N as length(a) + pos + 1, return substring in [N, N + count)
- If count is greater than length(a), return the whole string

## Timestamp

Function| Description |
----  |  ----|
int now()  |Return the current date and time |