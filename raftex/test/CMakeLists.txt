add_executable(
    file_based_wal_test
    FileBasedWalTest.cpp
    $<TARGET_OBJECTS:raftex_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
)
target_link_libraries(
    file_based_wal_test
    folly
    gtest
    glog
    gflags
    boost_context
    event
    double-conversion
    dl
    event
    -pthread
)
add_test(NAME file_based_wal_test COMMAND file_based_wal_test)

