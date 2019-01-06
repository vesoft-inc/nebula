add_executable(
    file_based_wal_test
    FileBasedWalTest.cpp
    $<TARGET_OBJECTS:wal_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
)
target_link_libraries(
    file_based_wal_test
    folly
    gtest
    glog
    gflags
    boost_context
    boost_system
    resolv
    double-conversion
    event
    dl
    -pthread
)
add_test(NAME file_based_wal_test COMMAND file_based_wal_test)

