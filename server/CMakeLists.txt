add_library(graph_db_handler OBJECT GraphDbServiceHandler.cpp)
add_dependencies(graph_db_handler common vgraph_thrift_obj)

add_executable(
    vgraphd
    GraphDbDaemon.cpp
    $<TARGET_OBJECTS:graph_db_handler>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:vgraph_thrift_obj>
    $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(
    vgraphd
    ${THRIFT_LIBRARIES}
    wangle
    folly
    boost_system
    boost_context
    ${OPENSSL_LIBRARIES}
    ${KRB5_LIBRARIES}
    glog
    gflags
    event
    ${COMPRESSION_LIBRARIES}
    resolv
    double-conversion
    dl
    -pthread
)

#add_subdirectory(test)

