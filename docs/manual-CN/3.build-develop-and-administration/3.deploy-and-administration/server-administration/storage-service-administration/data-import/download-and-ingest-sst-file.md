# Download and Ingest

Nebula 存储访问默认使用 `RocksDB` 作为 `key-value` 存储引擎。因此在大量数据加载时，可以通过运行一个 map-reduce job 离线生成 RocksDB 的 SST 文件，再直接分发到服务器上。

Nebula 提供了 `Spark-SSTFile-Generator` 工具。

`Spark-SSTFile-Generator` 通过映射文件，从 hive 表生成 SST 文件。

具体用法详见 [Spark application command line reference](https://github.com/vesoft-inc/nebula/blob/master/src/tools/spark-sstfile-generator/README.md)。

执行后会在 `HDFS` 上生成 SST 文件，目录结构如下：

```
|---1 (this is partition number)
|        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
|        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
|---2
         | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
         | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
....
```

其中各个目录为 partition 编号。

SST 文件名格式为 `{TYPE}-${FIRST_KEY_IN_THIS_FILE}.sst`，其中 `TYPE` 表示数据类型，`FIRST_KEY_IN_THIS_FILE` 为文件中的起始 key。（如果你想自己写工具生成 SST 文件，需要保证每个 `SST`  文件中的 key 是有序的。)

请确认所有 server 已安装 `Hadoop`，并且 `HADOOP_HOME ` 已设置。

运行 nebula console，执行 Download 命令：

```bash
nebula > DOWNLOAD HDFS "hdfs://${HADOOP_HOST}:${HADOOP_PORT}/${HADOOP_PATH}"
```

通过 `download` 命令以及各个 `Storage Server` 的 `meta` 信息，分别下载各自的 `SST` 文件到 `data/download` 目录中。其中：

- HADOOP_HOST 指定 Hadoop NameNode 地址
- HADOOP_PORT 指定 Hadoop NameNode 端口号
- HADOOP_PATH 指定 Hadoop 数据存放目录

如果 `download` 过程出现错误，请删除 `data/download` 目录下相应的数据文件，并尝试重新下载。如果遇到多次失败，请在 [GitHub](https://github.com/vesoft-inc/nebula/issues) 给提 issue。

数据下载完毕后，重新执行该命令不会发生任何操作。

SST 数据离线下载完成后，通过 `INGEST` 命令在线**加载**到存储服务中。

Ingest 命令如下：

```bash
nebula > INGEST
```

该命令将加载 `download` 目录中的 `SST` 文件。

**注意：**数据量较大时 `ingest` 会阻塞 `RocksDB`，请避免在请求高峰执行该命令。