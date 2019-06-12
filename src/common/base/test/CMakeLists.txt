add_executable(cord_test CordTest.cpp $<TARGET_OBJECTS:base_obj>)
nebula_link_libraries(
    cord_test
    gtest
)
nebula_add_test(cord_test)


add_executable(cord_bm CordBenchmark.cpp $<TARGET_OBJECTS:base_obj>)
nebula_link_libraries(
    cord_bm
    follybenchmark
    boost_regex
)

add_executable(murmurhash2_test MurmurHash2Test.cpp $<TARGET_OBJECTS:base_obj>)
nebula_link_libraries(
    murmurhash2_test
    gtest
    gtest_main
)
nebula_add_test(murmurhash2_test)

add_executable(
    configuration_test
    ConfigurationTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    configuration_test
    gtest
    gtest_main
)
nebula_add_test(configuration_test)

add_executable(
    status_test
    StatusTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    status_test
    gtest
    gtest_main
)
nebula_add_test(status_test)

add_executable(
    status_or_test
    StatusOrTest.cpp
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    status_or_test
    gtest
    gtest_main
)
nebula_add_test(status_or_test)

add_executable(
    either_or_test
    EitherOrTest.cpp
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    either_or_test
    gtest
    gtest_main
)
nebula_add_test(either_or_test)

add_executable(
    error_or_test
    ErrorOrTest.cpp
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    error_or_test
    gtest
    gtest_main
)
nebula_add_test(error_or_test)

add_executable(hash_bm HashBenchmark.cpp $<TARGET_OBJECTS:base_obj>)
nebula_link_libraries(
    hash_bm
    follybenchmark
    boost_regex
)

add_executable(signal_handler_test SignalHandlerTest.cpp $<TARGET_OBJECTS:base_obj>)
nebula_link_libraries(
    signal_handler_test
    gtest
    gtest_main
)
nebula_add_test(signal_handler_test)

add_executable(
    nebulakey_utils_test
    NebulaKeyUtilsTest.cpp
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    nebulakey_utils_test
    gtest
    gtest_main
)
nebula_add_test(nebulakey_utils_test)

add_executable(
    range_vs_transform_bm
    RangeVsTransformBenchmark.cpp
    $<TARGET_OBJECTS:base_obj>
)
nebula_link_libraries(
    range_vs_transform_bm
    follybenchmark
    boost_regex
)
target_compile_options(range_vs_transform_bm PRIVATE -O3)
