# 时间戳类型
- 时间戳类型的取值范围为 '1970-01-01 00:00:01' UTC  到  '2262-04-11 23:47:16' UTC

- 插入数据的时候，支持插入方式
    - 调用函数 now()
    - 时间字符串， 例如："2019-10-01 10:00:00"
    - 直接输入时间戳，即从 1970-01-01 00:00:00 开始的秒数

- 做数据存储的时候，会先将时间转化为**UTC时间**，读取的时候会将存储的**UTC时间**转换为**本地时间**给用户

- 底层存储数据类型为: **int64**

**示例**

先创建一个各为 school 的 tag

```
nebula> CREATE TAG school(name string , create_time timestamp);
```

插入一个名为"xiwang"，建校时间为"2010-09-01 08:00:00"的点

```
nebula> INSERT VERTEX school(name, create_time) VALUES hash("xiwang"):("xiwang", "2010-09-01 08:00:00")
```

插入一个名为"guangming"，建校时间为现在的点

```
nebula> INSERT VERTEX school(name, create_time) VALUES hash("guangming"):("guangming", now())
```
