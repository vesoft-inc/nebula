# Guide

Guided by a mapping file, `sst` files are generated from hive tables data source. A mapping file maps hive tables to vertexes and edges.
Multiple vertexes or edges can map to a single hive table, where a partition column will be used to distinguish different
vertexes and edges.  

Hive tables generated periodically by the upstream system and can reflect the latest data at present, and be
partitioned by a time column to indicate the time when data is generated.

To complete the task, *$HADOOP_HOME* env needs to be set correctly.  

## Environment

Component|Version
---|---
os|CentOS 6.5 final(kernel 2.6.32-431.el6.x86_64)
spark|1.6.2
hadoop|2.7.4
jdk|1.8+
scala|2.10.5
sbt|1.2.8

## Spark-Submit Command Line Reference

This is what we used in production environment:

```bash
${SPARK_HOME}/bin/spark-submit --master yarn \
                               --queue ${QUEUE_NAME} \
                               --conf spark.executor.instances=${EXECUTOR_NUM} \
                               --conf spark.executor.memory=${EXECUTOR_MEMORY} \
                               --conf spark.executor.cores=${EXECUTOR_CORES} \
                               --conf spark.executorEnv.LD_LIBRARY_PATH=${LD_LIBRARY_PATH} \
                               --conf spark.driver.extraJavaOptions=${EXTRA_OPTIONS} \
                               --class com.vesoft.tools.SparkSstFileGenerator \
                               --files ${MAPPING_FILE} \
                               nebula-spark-sstfile-generator.jar \
                               -di ${LATEST_DATE} \
                               -mi ${MAPPING_FILE} \
                               -pi ${PARTITION_FIELD} \
                               -so ${SSTFILE_OUTPUT_PATH}
```

The application options are described as follows.

### Spark Application Command Line Reference

We keep a convention when naming the option, those suffixed with _i_ will be an INPUT type option, while those suffixed with _o_ will be an OUTPUT type option.

```bash
usage: nebula spark sst file generator
 -ci,--default_column_mapping_policy <arg>   If omitted, use the policy when mapping column to property, all columns except primary_key's will be mapped to tag's property with the same name by default
 -di,--latest_date_input <arg>               Latest date to query, date format is YYYY-MM-dd
 -hi,--string_value_charset_input <arg>      When the value is string, the default charset encoding is UTF-8
 -ho,--hdfs_sst_file_output <arg>            Hdfs directory of SST files should not start with file:///
 -li,--limit_input <arg>                     Return limited number of edges/vertexes, usually used in POC stage, when omitted, Nebula Graph fetches all data.
 -mi,--mapping_file_input <arg>              Hive tables to nNebula Graph schema mapping file
 -pi,--date_partition_input <arg>            A partition field of type String of hive table, which represent a Date, and has format of YYY-MM-dd
 -ri,--repartition_number_input <arg>        Repartition number. Optimization trick is adapted to improve generation speed and data skewness. Need tuning to suit your data.
 -so,--local_sst_file_output <arg>           Local directories of the generated sst files should start with file:///
 -ti,--datasource_type_input <arg>           Currently supported source data types are [hive|hbase|csv], and the default type is hive
```

### Mapping File Schema

The format of the mapping files is json. File Schema is provided as [mapping-schema.json](mapping-schema.json) according to [Json Schema Standard](http://json-schema.org).
Here is an example of mapping file: [mapping.json](mapping.json)

## FAQ

### How to Use libnebula-native-client.so under CentOS6.5(2.6.32-431 x86-64)

1. Don't use officially distributed librocksdbjni-linux64.so, build locally on CentOS6.5.

```bash
DEBUG_LEVEL=0 make shared_lib
DEBUG_LEVEL=0 make rocksdbjava
```

*Make sure to keep consistent with DEBUG_LEVEL when building, or there will be some link error like `symbol not found`.*

2. Run `sbt assembly` to package this project to a spark job jar, whose default name is: `nebula-spark-sstfile-generator.jar`.
3. Run `jar uvf nebula-spark-sstfile-generator.jar librocksdbjni-linux64.so libnebula_native_client.so` to replace the `*.so` files packaged inside the dependency org.rocksdb:rocksdbjni:5.17.2,
    or the following error will occur when spark-submit:

```text
*** glibc detected *** /soft/java/bin/java: free(): invalid pointer: 0x00007f7985b9f0a0 ***
======= Backtrace: =========
/lib64/libc.so.6(+0x75f4e)[0x7f7c7d5e6f4e]
/lib64/libc.so.6(+0x78c5d)[0x7f7c7d5e9c5d]
/tmp/librocksdbjni3419235685305324910.so(_ZN7rocksdb10EnvOptionsC1Ev+0x578)[0x7f79431ff908]
/tmp/librocksdbjni3419235685305324910.so(Java_org_rocksdb_EnvOptions_newEnvOptions+0x1c)[0x7f7943044dbc]
[0x7f7c689c1747]
```

## TODO

1. Add database_name property to graph space level and tag/edge level,with the latter will override the former when provided in both levels.
2. Schema column definitions' order is important, keep it when parsing mapping file and encoding.
3. Integrated build with maven or cmake, where this spark assembly should be built after nebula native client
4. To handle the following situation: different tables share a common tag, like one with properties of (start_time, end_time).
