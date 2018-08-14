add_library(console_obj OBJECT CliManager.cpp CmdProcessor.cpp)
add_dependencies(console_obj common)

add_executable(
    vgraph
    GraphDbConsole.cpp
    $<TARGET_OBJECTS:console_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:vgraph_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(
    vgraph
    thriftcpp2
    thrift
    thrift-core
    thriftprotocol
    async
    protocol
    transport
    concurrency
    security
    thriftfrozen2
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    event
    bz2
    snappy
    zstd
    z
    glog
    gflags
    resolv
    double-conversion
    dl
    -pthread
)

#add_subdirectory(test)

