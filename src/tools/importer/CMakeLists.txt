add_custom_target(
    importer ALL
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${CMAKE_CURRENT_SOURCE_DIR} ">" "./_build.log" "2>&1"
    DEPENDS java-client
)
