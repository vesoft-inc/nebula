nebula_add_test(
    NAME stats_manager_test
    SOURCES StatsManagerTest.cpp
    OBJECTS $<TARGET_OBJECTS:stats_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest
)


nebula_add_executable(
    NAME stats_manager_bm
    SOURCES StatsManagerBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:stats_obj>
    LIBRARIES follybenchmark boost_regex
)
