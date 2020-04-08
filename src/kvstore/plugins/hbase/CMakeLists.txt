set(THRIFT1 ${Fbthrift_BIN})
set(THRIFT_TEMPLATES ${Fbthrift_INCLUDE_DIR}/thrift/templates)
include(ThriftGenerate)

set(type "OBJECT")
if (ENABLE_TESTING AND NOT ENABLE_ONLY_SHARED)
    set(type "ALL")
elseif (ENABLE_TESTING OR ENABLE_ONLY_SHARED)
    set(type "SHARED")
elseif (NOT ENABLE_ONLY_SHARED)
    set(type "OBJECT")
endif()

# Target object name : hbase_thrift
thrift_generate("hbase" "THBaseService" ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} "hbase" ${type})

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
