add_executable(
    stats_manager_test
    StatsManagerTest.cpp
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:base_obj>
)
target_link_libraries(
    stats_manager_test
    folly
    gtest
    glog
    gflags
    double-conversion
    dl
    -pthread
)
nebula_add_test(stats_manager_test)


add_executable(
    stats_manager_bm
    StatsManagerBenchmark.cpp
    $<TARGET_OBJECTS:stats_obj>
)

target_link_libraries(
    stats_manager_bm
    follybenchmark
    folly
    glog
    gflags
    boost_regex
    double-conversion
    event
    dl
    -pthread
)
