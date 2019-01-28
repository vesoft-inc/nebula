add_library(console_obj OBJECT CliManager.cpp CmdProcessor.cpp)
add_dependencies(console_obj base_obj)

add_executable(
    nebula
    NebulaConsole.cpp
    $<TARGET_OBJECTS:console_obj>
    $<TARGET_OBJECTS:client_cpp_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:graph_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:thread_obj>
)
target_link_libraries(
    nebula
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
    ${Readline_LIBRARY}
    ${NCURSES_LIBRARY}
    dl
    -pthread
)

install(TARGETS nebula DESTINATION bin)

#add_subdirectory(test)

