# $1: the path of thrift-1.0-SNAPSHOT.jar
# $2: the path of pom.xml
mvn compile package -DJAVA_FBTHRIFT_JAR=$1 -f $2/pom.xml
