FROM vesoft/nebula-dev:centos7 as builder

ENV JAVA_HOME='/usr/lib/jvm/java-1.8.0'
ENV JRE_HOME="$JAVA_HOME/jre"
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV CLASSPATH=".:$JAVA_HOME/lib"

COPY . /home/nebula/BUILD

RUN  .  ~/.bashrc \
    && cd /home/nebula/BUILD \
    && cmake . -DENABLE_NATIVE=ON \
    && make -j$(nproc) \
    && cd /home/nebula/BUILD/src/tools/native-client \
    && mvn install \
    \
# Build Spark-SSTfile-Generator
    && curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo \
    && yum install -y sbt \
    && cd /home/nebula/BUILD/src/tools/spark-sstfile-generator \
    && sbt assembly \
    \
# Build rocksdbjni
    && cd /home/nebula/ \
    && yum install -y bzip2-devel \
    && wget -qO- https://codeload.github.com/facebook/rocksdb/tar.gz/v5.15.10 | tar -xvzf - \
    && cd rocksdb-5.15.10 \
    && DEBUG_LEVEL=0 make -j$(nproc) shared_lib rocksdbjava \
    \
# Replace
    && cd /home/nebula \
    && cp BUILD/src/tools/native-client/_build/libnebula_native_client.so . \
    && cp BUILD/src/tools/spark-sstfile-generator/target/scala-*/nebula-spark-sstfile-generator.jar . \
    && cp rocksdb-5.15.10/java/target/librocksdbjni-linux64.so . \
    && jar -uvf nebula-spark-sstfile-generator.jar librocksdbjni-linux64.so libnebula_native_client.so

FROM centos:7

COPY --from=builder /home/nebula/nebula-spark-sstfile-generator.jar /root
COPY --from=builder /home/nebula/BUILD/src/tools/spark-sstfile-generator/mapping.json /root
COPY --from=builder /home/nebula/BUILD/src/tools/spark-sstfile-generator/invoke.sh /root

ENV JAVA_HOME='/usr/lib/jvm/jre-1.8.0-openjdk'
ENV JRE_HOME="$JAVA_HOME"
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV CLASSPATH=".:$JAVA_HOME/lib"

ENV HADOOP_HOME='/root/hadoop-2.7.7'
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

ENV SPARK_HOME='/root/spark-1.6.2-bin-hadoop2.6'

WORKDIR /root

# Just Hint the yarn resource manager port
# In fact, the container is client, so you can't bind this port which used by
# yarn resource manager, you need run this container and yarn in the same
# network bridge!
EXPOSE 8032

RUN yum update -y \
    && yum install -y java-1.8.0-openjdk wget \
    \
# Hadoop
    && wget -qO- https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz | tar -xvzf - \
    \
# Spark
    && wget -qO- https://archive.apache.org/dist/spark/spark-1.6.2/spark-1.6.2-bin-hadoop2.6.tgz | tar -xvzf - \
    && yum clean all \
    && rm -rf /var/cache/yum

ENTRYPOINT ["/root/invoke.sh"]
