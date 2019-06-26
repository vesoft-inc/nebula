get_property(java-fbthrift-jar GLOBAL PROPERTY java-fbthrift-jar)
add_custom_target(
    importer ALL
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${java-fbthrift-jar} ${CMAKE_CURRENT_SOURCE_DIR} ">" "./_build.log" "2>&1"
    DEPENDS java-client
)
