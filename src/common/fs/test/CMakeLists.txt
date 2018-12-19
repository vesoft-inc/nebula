add_executable(
    file_utils_test
    FileUtilsTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    file_utils_test
    folly
    gtest
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME file_utils_test COMMAND file_utils_test)

add_executable(
    temp_dir_test
    TempDirTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    temp_dir_test
    folly
    gtest
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME temp_dir_test COMMAND temp_dir_test)

add_executable(
    temp_file_test
    TempFileTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    temp_file_test
    folly
    gtest
    gtest_main
    glog
    gflags
    double-conversion
    dl
    -pthread
)
add_test(NAME temp_file_test COMMAND temp_file_test)

