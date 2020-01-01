execute_process(
    COMMAND
        ${CMAKE_SOURCE_DIR}/third-party/build-third-party.sh
    WORKING_DIRECTORY
        ${CMAKE_BINARY_DIR}
)
set(NEBULA_THIRDPARTY_ROOT ${CMAKE_BINARY_DIR}/third-party/install)
