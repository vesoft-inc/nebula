set(TIME_TEST_LIBS
    base_lib
    time_lib
    thread_lib
)
nebula_add_test(
    NAME
        duration_test
    SOURCES
        DurationTest.cpp
    LIBRARIES
        ${TIME_TEST_LIBS}
        gtest
)

nebula_add_test(
    NAME
        wallclock_test
    SOURCES
        WallClockTest.cpp
    LIBRARIES
        time_lib
        thread_lib
        gtest
)

nebula_add_executable(
    NAME
        duration_bm
    SOURCES
        DurationBenchmark.cpp
    LIBRARIES
        ${TIME_TEST_LIBS}
        follybenchmark
        boost_regex
)

nebula_add_executable(
    NAME wallclock_bm
    SOURCES WallClockBenchmark.cpp
    LIBRARIES
        time_lib
        follybenchmark
        boost_regex
)
