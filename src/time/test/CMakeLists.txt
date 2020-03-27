nebula_add_test(
    NAME
        duration_test
    SOURCES
        DurationTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_test(
    NAME
        wallclock_test
    SOURCES
        WallClockTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        gtest
)

nebula_add_executable(
    NAME
        duration_bm
    SOURCES
        DurationBenchmark.cpp
    OBJECTS
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:time_obj>
        $<TARGET_OBJECTS:thread_obj>
    LIBRARIES
        follybenchmark
        boost_regex
)

nebula_add_executable(
    NAME wallclock_bm
    SOURCES WallClockBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:time_obj>
    LIBRARIES follybenchmark boost_regex
)
