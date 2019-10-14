Nebula 通过 `DOWNLOAD` 语句从 `HDFS` 下载SST文件到download目录。

请确认 `Hadoop` 已安装，且 `HADOOP_HOME` 已配置

```
> echo $HADOOP_HOME
```

### 数据下载

```
nebula > DOWNLOAD HDFS "hdfs://${HADOOP_HOST}:${HADOOP_PORT}/${HADOOP_PATH}"
```
