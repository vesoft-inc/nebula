# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
)

target_link_libraries(
        file_utils_test
        PRIVATE
    fs_obj base_obj
    gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    LIBRARIES fs_obj base_obj
    LIBRARIES gtest
)
target_link_libraries(
        temp_dir_test
        PRIVATE
    fs_obj base_obj
    gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
)
target_link_libraries(
        temp_file_test
        PRIVATE
    fs_obj base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

