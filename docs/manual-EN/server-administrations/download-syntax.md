Nebula downloads `SST` files into the download directory with `DOWNLOAD` command.

Please make sure `Hadoop` have been installed and `HADOOP_HOME` have been set.

```
> echo $HADOOP_HOME
```

### Download data

```
nebula > DOWNLOAD HDFS "hdfs://${HADOOP_HOST}:${HADOOP_PORT}/${HADOOP_PATH}"
```

