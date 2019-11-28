# Download and Ingest

**Nebula Graph** uses `RocksDB` as the default `key-value` storage engine. Therefore, when you are trying to load a large amount of data, the SST files of RocksDB can be generated offline by running a map-reduce job and distributed directly to the servers.

**Nebula Graph** provides `Spark-SSTFile-Generator` tool.

`Spark-SSTFile-Generator` generates SST files from the hive table via the mapping files. For details on how to use it, please refer [Spark application command line reference](https://github.com/vesoft-inc/nebula/blob/master/src/tools/spark-sstfile-generator/README.md).

The execution will generate SST files on `HDFS`. The directory structure is as follows:

```plain
|---1 (this is partition number)
|        | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
|        | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
|---2
         | ---- vertex-${FIRST_KEY_IN_THIS_FILE}.sst
         | ---- edge-${FIRST_KEY_IN_THIS_FILE}.sst
....
```

Each directory is a partition number.

SST file name format is `{TYPE}-${FIRST_KEY_IN_THIS_FILE}.sst`, where `TYPE` is data type, `FIRST_KEY_IN_THIS_FILE` is the start Key of the file. (If you want to write your own tools to generate SST files, you need to ensure that the keys in each `SST` file are ordered.)

Please confirm that all servers have `Hadoop` installed and `HADOOP_HOME` set.

Run **Nebula Graph** console and execute the download command:

```ngql
nebula > DOWNLOAD HDFS "hdfs://${HADOOP_HOST}:${HADOOP_PORT}/${HADOOP_PATH}"
```

Download the `SST` files of each server into directory `data/download` respectively via the `DOWNLOAD` command and `meta` in the storage servers. Explanation of the above command:

- HADOOP_HOST specifies Hadoop NameNode address
- HADOOP_PORT specifies Hadoop NameNode port number
- HADOOP_PATH specifies Hadoop data storage directory

If error occurs when downloading, delete the corresponding data files in `data/download` directory and try to download again. If error occurs again, please raise us an issue on [GitHub](https://github.com/vesoft-inc/nebula/issues). When data download is done, re-execute the command leads to no actions.

When the offline SST data download is done, it can be ingested into the storage service via `INGEST` command.
`INGEST` command is as follows:

```ngql
nebula > INGEST
```

The command will ingest the `SST` files in `data/download` directory.

**Note:** `ingest` will block `RocksDB` when the data amount is large, please avoid running the command at requirement peak.
