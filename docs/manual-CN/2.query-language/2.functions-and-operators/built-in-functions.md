# 内建函数

**Nebula Graph** 支持在表达式中调用如下类型的内建函数。

## 数学相关

函数| 描述 |
----  |  ----|
double abs(double x) | 返回绝对值    |
double floor(double x) | 返回小于参数的最大整数（向下取整）   |
double ceil(double x) | 返回大于参数的最小整数（向上取整）  |
double round(double x) | 对参数取整，如果参数位于中间位置，则返回远离 0 的数字 |
double sqrt(double x) | 返回参数的平方根 |
double cbrt(double x) | 返回参数的立方根 |
double hypot(double x, double x) | 返回一个正三角形的斜边 |
double pow(double x, double y) | 返回 x 的 y 次幂 |
double exp(double x) | 计算 e 的 x 次幂 |
double exp2(double x) | 返回 2 的指定次方 |
double log(double x) | 返回参数的自然对数 |
double log2(double x) | 返回底数为 2 的对数 |
double log10(double x) | 返回底数为 10 的对数 |
double sin(double x) | 返回正弦函数值 |
double asin(double x) | 返回反正弦函数值
double cos(double x) | 返回余弦函数值 |
double acos(double x) | 返回反余弦函数值 |
double tan(double x) | 返回正切函数值 |
double atan(double x) | 返回反正切函数值 |
int rand32() | 返回 32bit 整型伪随机数 |
int rand32(int max) | 返回 [0, max) 区间内的 32bit 整型伪随机数 |
int rand32(int min, int max) | 返回 [min, max) 区间内的 32bit 整型伪随机数 |
int rand64() | 返回 64bit 整型伪随机数 |
int rand64(int max) | 返回 [0, max) 区间内的 64bit 整型伪随机数 |
int rand64(int min, int max) | 返回 [min, max) 区间内的 64bit 整型伪随机数 |

## 字符串相关

**注意：** 和 SQL 一样，nGQL 的字符索引（位置）从 `1` 开始，而不是类似 C 语言从 `0` 开始。

函数| 描述 |
----  |  ----|
int strcasecmp(string a, string b) | 大小写不敏感的字符串比较，相等时返回零，a > b 时返回值大于零，否则返回值小于零 |
string lower(string a) | 将字符串转换为小写 |
string upper(string a) | 将字符串转换为大写 |
int length(string a) | 返回字符串长度（整数）（目前实现为，返回占用字节数） |
string trim(string a) | 删除字符串两端的空白字符（空格，换行，制表符等） |
string ltrim(string a) | 删除字符串起始的空白字符 |
string rtrim(string a) | 删除字符串末尾的空白字符 |
string left(string a, int count) | 返回 [1, count] 范围内的子串，若字符串长度小于 count ，则返回原字符串 |
string right(string a, int count) | 返回 [size - count + 1, size] 范围内的子串，若字符串长度小于 count ，则返回原字符串 |
string lpad(string a, int size, string letters) | 使用字符串 letters 从左侧填充字符串至其长度不小于 size |
string rpad(string a, int size, string letters)| 使用字符串 letters 从右侧填充字符串至其长度不小于 size  |
string substr(string a, int pos, int count) | 从指定起始位置 pos 开始，获取长度为 count 的子串 |
int hash(string a) | 对数值进行 hash，返回值类型为整数 |

函数 `substr` 返回结果**注释**：

- 如果 pos 等于 0 ，返回空串
- 如果 pos 绝对值大于原字符串的长度，返回空串
- 如果 pos 大于 0 ，返回 [pos, pos + count) 范围内的子串
- 如果 pos 小于 0 ，设起始位置 N 为 length(a) + pos + 1 ，返回 [N, N + count) 范围内的子串
- 如果 count 大于 length(a) ，则返回整个字符串

## 时间相关

函数| 描述 |
---   |  ---|
int now()  |返回当前时间戳 |
