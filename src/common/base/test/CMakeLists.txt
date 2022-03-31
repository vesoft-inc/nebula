# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME cord_test
    SOURCES CordTest.cpp
)
find_package(Threads)
target_link_libraries(
        cord_test
        PRIVATE
        base_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)


nebula_add_test(
    NAME icord_test
    SOURCES ICordTest.cpp
)
target_link_libraries(
        icord_test
        PRIVATE
        base_obj
        gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME icord_bm
    SOURCES ICordBenchmark.cpp
    LIBRARIES base_obj
    LIBRARIES follybenchmark boost_regex
)
target_link_libraries(
        icord_bm
        PRIVATE
        base_obj
        gtest
    follybenchmark
    boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME logging_bm
    SOURCES LoggingBenchmark.cpp
    LIBRARIES base_obj
    LIBRARIES follybenchmark boost_regex
)
target_link_libraries(
        logging_bm
        PRIVATE
        base_obj
        gtest
    follybenchmark
    boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME murmurhash2_test
    SOURCES MurmurHash2Test.cpp
)
target_link_libraries(
        murmurhash2_test
        PRIVATE
        base_obj
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME status_test
    SOURCES StatusTest.cpp
    LIBRARIES fs_obj base_obj
    LIBRARIES gtest gtest_main
)
target_link_libraries(
        status_test
        PRIVATE
    fs_obj
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME status_or_test
    SOURCES StatusOrTest.cpp
)

target_link_libraries(
        status_or_test
        PRIVATE
    fs_obj
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
    NAME either_or_test
    SOURCES EitherOrTest.cpp
)
target_link_libraries(
        either_or_test
        PRIVATE
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME error_or_test
    SOURCES ErrorOrTest.cpp
)
target_link_libraries(
        error_or_test
        PRIVATE
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME hash_bm
    SOURCES HashBenchmark.cpp
    LIBRARIES base_obj
    LIBRARIES follybenchmark boost_regex
)
target_link_libraries(
        hash_bm
        PRIVATE
    base_obj
follybenchmark
boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME signal_handler_test
    SOURCES SignalHandlerTest.cpp
)
target_link_libraries(
 signal_handler_test
        PRIVATE
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME slow_op_tracker_test
    SOURCES SlowOpTrackerTest.cpp
)
target_link_libraries(
 slow_op_tracker_test
        PRIVATE
    base_obj time_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)


nebula_add_test(
    NAME lru_test
    SOURCES ConcurrentLRUCacheTest.cpp
)
target_link_libraries(
 lru_test
        PRIVATE
    base_obj
    gtest
    gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_executable(
    NAME range_vs_transform_bm
    SOURCES RangeVsTransformBenchmark.cpp
)
target_compile_options(range_vs_transform_bm PRIVATE -O3)
target_link_libraries(
  range_vs_transform_bm
        PRIVATE
    base_obj
follybenchmark boost_regex
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
    NAME object_pool_test
    SOURCES ObjectPoolTest.cpp
)
target_link_libraries(
  object_pool_test
        PRIVATE
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
