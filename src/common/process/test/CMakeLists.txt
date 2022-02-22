# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        process_test
    SOURCES
        ProcessTest.cpp
    LIBRARIES
        process_obj
        fs_obj
        base_obj
    LIBRARIES
        gtest
        gtest_main
)
