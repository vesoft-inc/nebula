# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME
        duration_test
    SOURCES
        DurationTest.cpp
        ScopedTimerTest.cpp
)
target_link_libraries(
        duration_test
        PRIVATE
        base_obj
        time_obj
        thread_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME
        wallclock_test
    SOURCES
        WallClockTest.cpp
)
target_link_libraries(
        wallclock_test
        PRIVATE
        time_obj
        thread_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)


nebula_add_test(
    NAME
        timezone_info_test
    SOURCES
        TimezoneInfoTest.cpp
)
target_link_libraries(
        timezone_info_test
        PRIVATE
        base_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)


nebula_add_test(
    NAME
        time_utils_test
    SOURCES
        TimeUtilsTest.cpp
)
target_link_libraries(
        time_utils_test
        PRIVATE
        time_utils_obj
        thread_obj
        datatypes_obj
        fs_obj
        wkt_wkb_io_obj
        datetime_parser_obj
        base_obj
        storage_thrift_obj
        meta_thrift_obj
        common_thrift_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME
        duration_bm
    SOURCES
        DurationBenchmark.cpp
)
target_link_libraries(
        duration_bm
        PRIVATE
        time_obj
        thread_obj
        base_obj
        gtest
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME wallclock_bm
    SOURCES WallClockBenchmark.cpp
)


target_link_libraries(
        wallclock_bm
        PRIVATE
        time_obj
    follybenchmark boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
