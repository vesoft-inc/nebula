Guided by a mapping file, `sst` files are generated from hive tables datasource. A mapping file maps hive tables to vertexes and edges.
Multiple vertexes or edges can map to a single hive table, where a partition column will be used to distinguish different
vertexes and edges.  

Hive tables generated periodically by the upstream system can reflect the latest data at present, and be
partitioned by a time column to indicate the time when data are generated.

To complete the task, *$HADOOP_HOME* env needs to be set correctly.  

# Environment
component|version
---|---
os|centos6.5 final(kernel 2.6.32-431.el6.x86_64)
spark|1.6.2
hadoop|2.7.4
jdk|1.8+
scala|2.10.5
sbt|1.2.8


# Spark-submit command line reference
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
The application options are described as following.

# Spark application command line reference
We keep a convention when naming the option, those suffix with _i_ will be an INPUT type option, while those suffix with _o_ will be an OUTPUT type option.

```bash
usage: nebula spark sst file generator
 -ci,--default_column_mapping_policy <arg>   If omitted, use the policy when mapping column to property, all columns except primary_key's will be mapped to tag's property with the same name by default
 -di,--latest_date_input <arg>               Latest date to query, date format is YYYY-MM-dd
 -hi,--string_value_charset_input <arg>      When the value is string, the default charset encoding is UTF-8
 -ho,--hdfs_sst_file_output <arg>            Hdfs directory of SST files should not start with file:///
 -li,--limit_input <arg>                     Return limited number of edges/vertexes, usually used in POC stage, when omitted, fetch all data.
 -mi,--mapping_file_input <arg>              Hive tables to nebula graph schema mapping file
 -pi,--date_partition_input <arg>            A partition field of type String of hive table, which represent a Date, and has format of YYY-MM-dd
 -ri,--repartition_number_input <arg>        Repartition number. Optimization trick is adapted to improve generation speed and data skewness. Need tuning to suit your data.
 -so,--local_sst_file_output <arg>           Local directories of the generated sst files should start with file:///
 -ti,--datasource_type_input <arg>           Currently supported source data types are hive|hbase|csv], and the default type is hive
```

# Mapping file schema

The format of the mapping files is json. File Schema is provided as [mapping-schema.json](mapping-schema.json) according to [Json Schema Standard](http://json-schema.org).
Here is an example of mapping file: [mapping.json](mapping.json)

# Principle

The mapping from tables to V/E

```
t1 : |p1|p2|p3|p4|
                |                      |t2 V|
          --------                       ^ p4
          V                              |    p4          p7
t2 : |p5|p4|p7|p8|           =======>  |t1 V| --> |t2 V| --> |t3 V|
             |                                       | p7
       -------                                       V
       V                                           |t3 V|
t3 : |p7|p12|p13|p14|p15|
```

The mapping file map table to vertex, foreign key to edge, and flat the table records to graph.

# Docker container usage

1. You need have a hadoop environment in host;
2. Write a mapping json file which specify how transfer tables to graph;
3. `docker run -t --env-file=<envfile> --net=host vesoft/spark-sstfile-generator`,
the `envfile` specify the arguments in *invoker.sh*, such as spark executor configuration,
the mapping file path etc.
4. Finally the transform job will be submit to the hadoop cluster.

# FAQ
## How to use libnebula-native-client.so under CentOS6.5(2.6.32-431 x86-64)

1. Don't use officially distributed librocksdbjni-linux64.so, build locally on CentOS6.5.

```bash
DEBUG_LEVEL=0 make shared_lib
DEBUG_LEVEL=0 make rocksdbjava
```
*make sure to keep consistent with DEBUG_LEVEL when building, or there will be some link error like `symbol not found`*

2. run `sbt assembly` to package this project to a spark job jar, whose default name is: `nebula-spark-sstfile-generator.jar`  
3. run `jar uvf nebula-spark-sstfile-generator.jar librocksdbjni-linux64.so libnebula_native_client.so` to replace the `*.so` files packaged inside the dependency org.rocksdb:rocksdbjni:5.17.2,
    or the following error will occur when spark-submit:

```
*** glibc detected *** /soft/java/bin/java: free(): invalid pointer: 0x00007f7985b9f0a0 ***
======= Backtrace: =========
/lib64/libc.so.6(+0x75f4e)[0x7f7c7d5e6f4e]
/lib64/libc.so.6(+0x78c5d)[0x7f7c7d5e9c5d]
/tmp/librocksdbjni3419235685305324910.so(_ZN7rocksdb10EnvOptionsC1Ev+0x578)[0x7f79431ff908]
/tmp/librocksdbjni3419235685305324910.so(Java_org_rocksdb_EnvOptions_newEnvOptions+0x1c)[0x7f7943044dbc]
[0x7f7c689c1747]
```

# TODO
1. Add database_name property to graph space level and tag/edge level,with the latter will override the former when provided in both levels.
2. Schema column definitions' order is important, keep it when parsing mapping file and encoding.
3. Integrated build with maven or cmake, where this spark assembly should be built after nebula native client
4. To handle the following situation: different tables share a common tag, like one with properties of (start_time, end_time).
