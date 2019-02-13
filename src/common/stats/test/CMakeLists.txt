add_executable(
    stats_manager_test
    StatsManagerTest.cpp
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    stats_manager_test
    gtest
)
nebula_add_test(stats_manager_test)


add_executable(
    stats_manager_bm
    StatsManagerBenchmark.cpp
    $<TARGET_OBJECTS:stats_obj>
)
nebula_link_libraries(
    stats_manager_bm
    follybenchmark
    boost_regex
)
