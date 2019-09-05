add_custom_target(
    importer ALL
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${NEBULA_BUILD_VERSION} ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ">" "./_build.log" "2>&1"
    DEPENDS java-client
)

add_custom_target(
    clean-import
    COMMAND "rm" "-fr" "target" "_build.log"
)
