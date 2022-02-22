# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        hdfs_helper_test
    SOURCES
        HdfsHelperTest.cpp
    LIBRARIES
        hdfs_helper_obj
        process_obj
        fs_obj
        base_obj
    LIBRARIES
        gtest
        gtest_main
)
