#!/bin/bash
 
#-----------------------------------------------
# variable value
#-----------------------------------------------
# $1: the path of thrift-1.0-SNAPSHOT.jar
# $2: the path of java src
# $3: the path of current build dir
java_fbthrift_jar=$1
src_root_path=$2
output_src_file=$3
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
# check maven tool exist
#-----------------------------------------------

maven_check()
{
	command -v mvn >/dev/null 2>&1 || { echo >&2 "Require maven but it's not installed.  Aborting."; exit 1; }
}

#-----------------------------------------------
# setup dependent graph source files
#-----------------------------------------------
setup_graph_source()
{
	check_file_exist $graph_gen_java_path
	find  $java_client_source_file_path  -type l |xargs rm -rf {}
	ln -s $graph_gen_java_path/* $java_client_source_file_path
}

#-----------------------------------------------
# compile java-client
#-----------------------------------------------
compile_java_client()
{
	mvn clean package -DJAVA_FBTHRIFT_JAR=$java_fbthrift_jar -f ${pom_file_path}
}


setup_graph_source
compile_java_client

