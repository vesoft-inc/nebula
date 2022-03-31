# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        network_utils_test
    SOURCES
        NetworkUtilsTest.cpp
)

target_link_libraries(
        network_utils_test
        PRIVATE
        base_obj
        network_obj
        fs_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)


nebula_add_executable(
    NAME
        network_utils_bm
    SOURCES
        NetworkUtilsBenchmark.cpp
)
target_link_libraries(
        network_utils_bm
        PRIVATE
        base_obj
        network_obj
        fs_obj
        wangle
        fizz
        sodium
        follybenchmark
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
