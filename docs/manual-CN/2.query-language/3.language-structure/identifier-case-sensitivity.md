# 标识符大小写

## 用户自定义的标识符为大小写敏感

下方示例语句有错，因为`my_space` 和`MY_SPACE` 为两个不同的变量名。

```ngql
nebula> CREATE SPACE my_space;
nebula> use MY_SPACE;   ---- my_space 和 MY_SPACE 是两个不同的 space
```

## 关键词和保留关键词为大小写不敏感

下面四条语句是等价的（因为 show 和 spaces 都是保留字）

```ngql
nebula> show spaces;
nebula> SHOW SPACES;
nebula> SHOW spaces;
nebula> show SPACES;
```
