add_executable(
    file_utils_test
    FileUtilsTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    file_utils_test
    gtest
)
nebula_add_test(file_utils_test)

add_executable(
    temp_dir_test
    TempDirTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    temp_dir_test
    gtest
)
nebula_add_test(temp_dir_test)

add_executable(
    temp_file_test
    TempFileTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    temp_file_test
    gtest
    gtest_main
)
nebula_add_test(temp_file_test)

