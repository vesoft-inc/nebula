Nebula Graph 标识符为大小写敏感，因此下方示例语句无效，`my_space` 和`MY_SPACE` 为两个不同的space。

```
nebula> CREATE SPACE my_space;
nebula> use MY_SPACE;
```

但是关键词和保留关键词为大小写不敏感，因此如下语句是等价的：

```
nebula> show spaces;
nebula> SHOW SPACES;
nebula> SHOW spaces;
nebula> show spaces;
```
