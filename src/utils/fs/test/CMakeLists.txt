# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest
)

nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)
