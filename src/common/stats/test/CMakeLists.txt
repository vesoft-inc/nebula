set(STATS_TEST_LIBS
    stats_lib
    time_lib
    base_lib
    thread_lib
)

nebula_add_test(
    NAME
        stats_manager_test
    SOURCES
        StatsManagerTest.cpp
    LIBRARIES
        ${STATS_TEST_LIBS}
        gtest
)

nebula_add_test(
    NAME
        stats_manager_rate_test
    SOURCES
        StatsManagerRateTest.cpp
    LIBRARIES
        ${STATS_TEST_LIBS}
        gtest
)

nebula_add_test(
    NAME
        stats_manager_cross_level_test
    SOURCES
        StatsManagerCrossLevelTest.cpp
    LIBRARIES
        ${STATS_TEST_LIBS}
        gtest
)


nebula_add_executable(
    NAME
        stats_manager_bm
    SOURCES
        StatsManagerBenchmark.cpp
    LIBRARIES
        stats_lib
        time_lib
        base_lib
        follybenchmark
        boost_regex
)
