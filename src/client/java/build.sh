#!/bin/bash

# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

#-----------------------------------------------
# variable value
#-----------------------------------------------
# $1: the path of java src
# $2: the path of current build dir
src_root_path=$1
output_src_file=$2
pom_file_path=${src_root_path}/pom.xml
graph_gen_java_path=${output_src_file}/../../interface/gen-java/com/vesoft/nebula/graph/
java_client_source_file_path=${src_root_path}/src/main/java/com/vesoft/nebula/graph/

#-----------------------------------------------
# check file or dir exist
#-----------------------------------------------
check_file_exist()
{
    if [ -f $1 ] || [ -d $1 ]
    then
        echo "File or path exist : $1"
    else
        echo "File or path not exist : $1"
        exit 1;
    fi
}

#-----------------------------------------------
# setup dependent graph source files
#-----------------------------------------------
setup_graph_source()
{
    check_file_exist $graph_gen_java_path
    find  $java_client_source_file_path  -type l |xargs rm -rf {}
    ln -s $graph_gen_java_path/ $java_client_source_file_path/generated
}

#-----------------------------------------------
# compile java-client
#-----------------------------------------------
compile_java_client()
{
    mvn clean package -f ${pom_file_path}
}

setup_graph_source
compile_java_client

