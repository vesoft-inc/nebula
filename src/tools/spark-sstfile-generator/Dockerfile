from vesoft/nebula-dev:centos7

ENV JAVA_HOME='/usr/lib/jvm/java-1.8.0' JRE_HOME="$JAVA_HOME/jre" \
    PATH="$JAVA_HOME/bin:$PATH" CLASSPATH=".:$JAVA_HOME/lib"

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
    && wget -q https://codeload.github.com/facebook/rocksdb/zip/v5.15.10 \
    && unzip v5.15.10 \
    && cd rocksdb-5.15.10 \
    && DEBUG_LEVEL=0 make -j$(nproc) shared_lib rocksdbjava
