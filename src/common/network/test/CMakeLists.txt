# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        network_utils_test
    SOURCES
        NetworkUtilsTest.cpp
    LIBRARIES
        base_obj
        network_obj
        fs_obj
    LIBRARIES
        ${THRIFT_LIBRARIES}
        gtest
)


nebula_add_executable(
    NAME
        network_utils_bm
    SOURCES
        NetworkUtilsBenchmark.cpp
    LIBRARIES
        base_obj
        network_obj
        fs_obj
    LIBRARIES
        wangle
        fizz
        sodium
        follybenchmark
)
