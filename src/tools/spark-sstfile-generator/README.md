# mapping file format

conceptually, each hive table maps to a nebula vertex/edge, whose primary key should be identified


# TODO
1. add database_name property to graphspace level and tag/edge level, which the latter will override the former when provided in both levels
2. schema column definitions' order is important, keep it when parsing mapping file and when encoding
3. integrated build with maven or cmake, where this spark assembly should be build after nebula native client
4. to handle following situation: different tables share a common Tag, like a tag with properties of (start_time, end_time)


# How to use libnebula-native-client.so under CentOS6.5(2.6.32-431 x86-64)

1. build rocksdbjava natively under CentOS6.5, which will generate librocksdbjni-linux64.so
```bash
DEBUG_LEVEL=0 make shared_lib
DEBUG_LEVEL=0 make rocksdbjava
```
_make sure to keep consistent with DEBUG_LEVEL, or there will be some link error like `symbol not found`
2. run `sbt assembly` to package this project to a spark job jar, which is default named: `nebula-spark-sstfile-generator.jar`
3. run `jar uvf nebula-spark-sstfile-generator.jar librocksdbjni-linux64.so libnebula_native_client.so` to replace the `*.so` files packaged inside the dependency org.rocksdb:rocksdbjni:5.17.2,or some error like following will occur when spark-submit:
```
*** glibc detected *** /soft/java/bin/java: free(): invalid pointer: 0x00007f7985b9f0a0 ***
======= Backtrace: =========
/lib64/libc.so.6(+0x75f4e)[0x7f7c7d5e6f4e]
/lib64/libc.so.6(+0x78c5d)[0x7f7c7d5e9c5d]
/tmp/librocksdbjni3419235685305324910.so(_ZN7rocksdb10EnvOptionsC1Ev+0x578)[0x7f79431ff908]
/tmp/librocksdbjni3419235685305324910.so(Java_org_rocksdb_EnvOptions_newEnvOptions+0x1c)[0x7f7943044dbc]
[0x7f7c689c1747]
```

4. run spark-submit to submit spark job
This is what we used in production environment: 
```bash
/soft/spark/bin/spark-submit --master yarn --queue fmprod --conf spark.executor.instances=54 --conf spark.executor.memory=42g  --conf spark.executorEnv.LD_LIBRARY_PATH='/soft/server/nebula_native_client:/usr/local/lib:/usr/local/lib64' --conf spark.driver.extraJavaOptions='-Djava.library.path=/soft/server/nebula_native_client/:/usr/local/lib64:/usr/local/lib' --class com.vesoft.tools.SparkSstFileGenerator --files mapping.json nebula-spark-sstfile-generator.jar -li "2019-04-22" -mi mapping.json -pi dt -so nebula_output
```