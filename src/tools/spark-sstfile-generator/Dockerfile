from vesoft/nebula-dev:centos7

ENV JAVA_HOME='/usr/lib/jvm/java-1.8.0' JRE_HOME="$JAVA_HOME/jre" \
    PATH="$JAVA_HOME/bin:$PATH" CLASSPATH=".:$JAVA_HOME/lib"

COPY . /home/nebula/BUILD
#COPY ./src/tools/spark-sstfile-generator/repositories ~/.sbt/repositories

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
#    && sbt --version \
#    && echo -e '[repositories]\nlocal\nmaven-local:file://~/.m2/repository' > ~/.sbt/repositories \
    && cd /home/nebula/BUILD/src/tools/spark-sstfile-generator \
    && sbt assembly
