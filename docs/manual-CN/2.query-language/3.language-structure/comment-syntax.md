# 注释

**Nebula Graph** 支持四种注释方式：

* 在行末加 #
* 在行末加 --
* 在行末加 //，与 C 语言类似
* 添加 `/* */` 符号，其开始和结束序列无需在同一行，因此此类注释方式支持换行。

尚不支持嵌套注释。
注释方式示例如下：

```ngql
nebula> -- Do nothing this line
nebula> YIELD 1+1     # 注释在本行末结束
nebula> YIELD 1+1     -- 注释在本行末结束
nebula> YIELD 1+1     // 注释在本行末结束
nebula> YIELD 1  /* 这是行内注释 */ + 1
nebula> YIELD 11 +  \  
/* 多行注释使用      \
隔开  \
*/ 12
```

行内 `\` 表示换行符。
