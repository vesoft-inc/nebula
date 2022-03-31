# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        hdfs_helper_test
    SOURCES
        HdfsHelperTest.cpp
)
target_link_libraries(
        hdfs_helper_test
        PRIVATE
        hdfs_helper_obj
        process_obj
        fs_obj
        base_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
