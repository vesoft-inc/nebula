add_executable(
    file_based_wal_test
    FileBasedWalTest.cpp
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
)
nebula_link_libraries(
    file_based_wal_test
    gtest
)
nebula_add_test(file_based_wal_test)

