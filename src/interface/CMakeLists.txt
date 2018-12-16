add_custom_command(
    OUTPUT
        gen-cpp2/GraphService.cpp
        gen-cpp2/GraphServiceAsyncClient.cpp
        gen-cpp2/GraphService_processmap_binary.cpp
        gen-cpp2/GraphService_processmap_compact.cpp
        gen-cpp2/graph_constants.cpp
        gen-cpp2/graph_data.cpp
        gen-cpp2/graph_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"interface\",process_in_event_base,stack_arguments" "--gen" "java:hashcode" "--gen" "go"   "-o" "." "./graph.thrift"
    DEPENDS graph.thrift
)


add_custom_command(
    OUTPUT
        gen-cpp2/RaftexService.cpp
        gen-cpp2/RaftexServiceAsyncClient.cpp
        gen-cpp2/RaftexService_processmap_binary.cpp
        gen-cpp2/RaftexService_processmap_compact.cpp
        gen-cpp2/raftex_constants.cpp
        gen-cpp2/raftex_data.cpp
        gen-cpp2/raftex_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"interface\",process_in_event_base,stack_arguments" "--gen" "java:hashcode" "--gen" "go"   "-o" "." "./raftex.thrift"
    DEPENDS raftex.thrift
)


add_custom_command(
    OUTPUT
        gen-cpp2/StorageService.cpp
        gen-cpp2/StorageServiceAsyncClient.cpp
        gen-cpp2/StorageService_processmap_binary.cpp
        gen-cpp2/StorageService_processmap_compact.cpp
        gen-cpp2/storage_constants.cpp
        gen-cpp2/storage_data.cpp
        gen-cpp2/storage_types.cpp
    COMMAND "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/bin/thrift1" "--allow-neg-enum-vals" "--templates" "${CMAKE_HOME_DIRECTORY}/third-party/fbthrift/_install/include/thrift/templates" "--gen" "mstch_cpp2:include_prefix=\"interface\",process_in_event_base,stack_arguments" "--gen" "java:hashcode" "--gen" "go"   "-o" "." "./storage.thrift"
    DEPENDS storage.thrift
)


add_custom_target(
    clean-interface
    COMMAND "rm" "-fr" "gen-cpp2" "gen-java" "gen-go" "gen-py"
)


add_library(
    graph_thrift_obj OBJECT
    gen-cpp2/GraphService.cpp
    gen-cpp2/GraphServiceAsyncClient.cpp
    gen-cpp2/GraphService_processmap_binary.cpp
    gen-cpp2/GraphService_processmap_compact.cpp
    gen-cpp2/graph_constants.cpp
    gen-cpp2/graph_data.cpp
    gen-cpp2/graph_types.cpp
)
add_dependencies(graph_thrift_obj tgt_fbthrift)


add_library(
    raftex_thrift_obj OBJECT
    gen-cpp2/RaftexService.cpp
    gen-cpp2/RaftexServiceAsyncClient.cpp
    gen-cpp2/RaftexService_processmap_binary.cpp
    gen-cpp2/RaftexService_processmap_compact.cpp
    gen-cpp2/raftex_constants.cpp
    gen-cpp2/raftex_data.cpp
    gen-cpp2/raftex_types.cpp
)
add_dependencies(raftex_thrift_obj tgt_fbthrift)


add_library(
    storage_thrift_obj OBJECT
    gen-cpp2/StorageService.cpp
    gen-cpp2/StorageServiceAsyncClient.cpp
    gen-cpp2/StorageService_processmap_binary.cpp
    gen-cpp2/StorageService_processmap_compact.cpp
    gen-cpp2/storage_constants.cpp
    gen-cpp2/storage_data.cpp
    gen-cpp2/storage_types.cpp
)
add_dependencies(storage_thrift_obj tgt_fbthrift)
