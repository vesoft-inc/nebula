#! /usr/bin/env bash

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
                               -so ${SSTFILE_OUTPUT_PATH} \
                               -ho ${HDFS_OUTPUT_PATH}
