add_custom_command(
    OUTPUT
        gen-cpp2/THBaseService.cpp
        gen-cpp2/THBaseServiceAsyncClient.cpp
        gen-cpp2/THBaseService_processmap_binary.cpp
        gen-cpp2/THBaseService_processmap_compact.cpp
        gen-cpp2/hbase_constants.cpp
        gen-cpp2/hbase_data.cpp
        gen-cpp2/hbase_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--strict" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"hbase\",process_in_event_base,stack_arguments" "-o" "." "./hbase.thrift"
    DEPENDS hbase.thrift
)

add_library(
    hbase_thrift_obj OBJECT
    gen-cpp2/THBaseService.cpp
    gen-cpp2/THBaseServiceAsyncClient.cpp
    gen-cpp2/THBaseService_processmap_binary.cpp
    gen-cpp2/THBaseService_processmap_compact.cpp
    gen-cpp2/hbase_constants.cpp
    gen-cpp2/hbase_data.cpp
    gen-cpp2/hbase_types.cpp
)
add_dependencies(hbase_thrift_obj common_thrift_obj tgt_fbthrift)

add_library(
    hbasestore_obj OBJECT
    HBaseStore.cpp
    HBaseEngine.cpp
    HBaseClient.cpp
)
add_dependencies(hbasestore_obj hbase_thrift_obj base_obj meta_client)

add_subdirectory(test)

