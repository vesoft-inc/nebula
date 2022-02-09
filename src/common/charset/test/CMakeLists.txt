# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        charset_test
    SOURCES
        CharsetTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:charset_obj>
        $<TARGET_OBJECTS:base_obj>
    LIBRARIES
        gtest
        gtest_main
)
