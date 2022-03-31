# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        process_test
    SOURCES
        ProcessTest.cpp
)
target_link_libraries(
        process_test
        PRIVATE
        process_obj
        fs_obj
        base_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
