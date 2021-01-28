set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

# Target object name : hbase_thrift_obj
thrift_generate("hbase" "THBaseService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "hbase")

nebula_add_library(
    hbasestore_obj OBJECT
    HBaseStore.cpp
    HBaseClient.cpp
)

# HBase things will be removed later.
# nebula_add_subdirectory(test)

add_custom_target(
    clean-hbase
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py"
)
