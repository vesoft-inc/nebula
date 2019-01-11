#!/bin/bash
 
#-----------------------------------------------
# variable value
#-----------------------------------------------
src_root_path=`cd ../../../; pwd`;
thrift_root_path=$src_root_path/"third-party/fbthrift"
thrift_jar_source_path=$thrift_root_path/"_build/fbthrift-2018.08.20.00/thrift/lib/java/thrift"
graph_gen_java_path=$src_root_path"/src/interface/gen-java/nebula/graph/"
java_client_source_file_path=$src_root_path/src/client/java/src/main/java/nebula/graph/

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
# compile dependent thrift jar
#-----------------------------------------------
compile_thrift_jar()
{
	check_file_exist $thrift_jar_source_path
	maven_check
	cd $thrift_jar_source_path && mvn clean package && cd -
	echo `pwd`
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
	mvn clean package
}


compile_thrift_jar
setup_graph_source
compile_java_client


