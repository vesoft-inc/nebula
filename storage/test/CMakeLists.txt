add_executable(
    shard_test
    ShardTest.cpp
    $<TARGET_OBJECTS:storage_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:fs_obj>
)
target_link_libraries(
    shard_test
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
add_test(NAME shard_test COMMAND shard_test)

