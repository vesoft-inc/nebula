# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
    LIBRARIES fs_obj base_obj
    LIBRARIES gtest
)

nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    LIBRARIES fs_obj base_obj
    LIBRARIES gtest
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
    LIBRARIES fs_obj base_obj
    LIBRARIES gtest gtest_main
)
