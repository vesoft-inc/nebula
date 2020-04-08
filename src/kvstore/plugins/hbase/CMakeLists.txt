set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

# Target object name : hbase_thrift
thrift_generate("hbase" "THBaseService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "hbase")

nebula_add_all_library(
    hbasestore
    HBaseStore.cpp
    HBaseClient.cpp
)

nebula_add_subdirectory(test)

add_custom_target(
    clean-hbase
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py"
)
