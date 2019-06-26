get_property(java-fbthrift-jar GLOBAL PROPERTY java-fbthrift-jar)

add_custom_target(
    java-client ALL
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${java-fbthrift-jar} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR} ">" "./_build.log" "2>&1"
    DEPENDS graph_thrift_obj
)

add_custom_target(
    clean-java-client
    COMMAND "rm" "-fr" "target"
)

