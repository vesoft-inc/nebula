nebula_add_test(
    NAME cord_test
    SOURCES CordTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest
)

nebula_add_executable(
    NAME cord_bm
    SOURCES CordBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES follybenchmark boost_regex
)

nebula_add_executable(
    NAME logging_bm
    SOURCES LoggingBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES follybenchmark boost_regex
)

nebula_add_test(
    NAME murmurhash2_test
    SOURCES MurmurHash2Test.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME configuration_test
    SOURCES ConfigurationTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME status_test
    SOURCES StatusTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME status_or_test
    SOURCES StatusOrTest.cpp
    OBJECTS $<TARGET_OBJECTS:fs_obj> $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME either_or_test
    SOURCES EitherOrTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME error_or_test
    SOURCES ErrorOrTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_executable(
    NAME hash_bm
    SOURCES HashBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES follybenchmark boost_regex
)

nebula_add_test(
    NAME signal_handler_test
    SOURCES SignalHandlerTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME slow_op_tracker_test
    SOURCES SlowOpTrackerTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj> $<TARGET_OBJECTS:time_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME lru_test
    SOURCES ConcurrentLRUCacheTest.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES gtest gtest_main
)

nebula_add_executable(
    NAME range_vs_transform_bm
    SOURCES RangeVsTransformBenchmark.cpp
    OBJECTS $<TARGET_OBJECTS:base_obj>
    LIBRARIES follybenchmark boost_regex
)
target_compile_options(range_vs_transform_bm PRIVATE -O3)
