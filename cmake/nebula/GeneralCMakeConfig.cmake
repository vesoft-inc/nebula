# By default, all dynamic and static libraries will be placed at ${CMAKE_BINARY_DIR}/lib,
# while all executables at ${CMAKE_BINARY_DIR}/bin.
# But for the sake of cleanliness, all executables ending with `_test' will be placed
# at ${CMAKE_BINARY_DIR}/bin/test, while those ending with `_bm' at ${CMAKE_BINARY_DIR}/bin/bench.
# Please see `nebula_add_executable' for this rule.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

if(ENABLE_TESTING)
    enable_testing()
endif()

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
print_config(CMAKE_BUILD_TYPE)

if (${CMAKE_INSTALL_PREFIX} STREQUAL "/usr/local")
    set(CMAKE_INSTALL_PREFIX "/usr/local/nebula")
endif()
print_config(CMAKE_INSTALL_PREFIX)


set(CMAKE_SKIP_RPATH TRUE)
