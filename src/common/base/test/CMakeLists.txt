nebula_add_test(
    NAME cord_test
    SOURCES CordTest.cpp
    LIBRARIES base_lib gtest
)

nebula_add_executable(
    NAME cord_bm
    SOURCES CordBenchmark.cpp
    LIBRARIES base_lib follybenchmark boost_regex
)

nebula_add_executable(
    NAME logging_bm
    SOURCES LoggingBenchmark.cpp
    LIBRARIES base_lib follybenchmark boost_regex
)

nebula_add_test(
    NAME murmurhash2_test
    SOURCES MurmurHash2Test.cpp
    LIBRARIES base_lib gtest gtest_main
)

nebula_add_test(
    NAME configuration_test
    SOURCES ConfigurationTest.cpp
    LIBRARIES fs_lib base_lib gtest gtest_main
)

nebula_add_test(
    NAME status_test
    SOURCES StatusTest.cpp
    LIBRARIES fs_lib base_lib gtest gtest_main
)

nebula_add_test(
    NAME status_or_test
    SOURCES StatusOrTest.cpp
    LIBRARIES fs_lib base_lib gtest gtest_main
)

nebula_add_test(
    NAME either_or_test
    SOURCES EitherOrTest.cpp
    LIBRARIES base_lib gtest gtest_main
)

nebula_add_test(
    NAME error_or_test
    SOURCES ErrorOrTest.cpp
    LIBRARIES base_lib gtest gtest_main
)

nebula_add_executable(
    NAME hash_bm
    SOURCES HashBenchmark.cpp
    LIBRARIES base_lib follybenchmark boost_regex
)

nebula_add_test(
    NAME signal_handler_test
    SOURCES SignalHandlerTest.cpp
    LIBRARIES base_lib gtest gtest_main
)

nebula_add_test(
    NAME slow_op_tracker_test
    SOURCES SlowOpTrackerTest.cpp
    LIBRARIES base_lib time_lib gtest gtest_main
)

nebula_add_test(
    NAME lru_test
    SOURCES ConcurrentLRUCacheTest.cpp
    LIBRARIES base_lib gtest gtest_main
)

nebula_add_executable(
    NAME range_vs_transform_bm
    SOURCES RangeVsTransformBenchmark.cpp
    LIBRARIES base_lib follybenchmark boost_regex
)
target_compile_options(range_vs_transform_bm PRIVATE -O3)
