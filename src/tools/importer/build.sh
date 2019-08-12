# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

# $1: the path of thrift-1.0-SNAPSHOT.jar
# $2: the path of pom.xml
mvn compile package -DJAVA_FBTHRIFT_JAR=$1 -f $2/pom.xml
