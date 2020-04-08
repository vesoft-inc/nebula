nebula_add_test(
    NAME file_utils_test
    SOURCES FileUtilsTest.cpp
    LIBRARIES fs_lib base_lib gtest
)

nebula_add_test(
    NAME temp_dir_test
    SOURCES TempDirTest.cpp
    LIBRARIES fs_lib base_lib gtest
)

nebula_add_test(
    NAME temp_file_test
    SOURCES TempFileTest.cpp
    LIBRARIES fs_lib base_lib gtest gtest_main
)
