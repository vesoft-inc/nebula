set(FILTER_TEST_LIBS
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:base_obj>
)


nebula_add_test(
    NAME expression_test
    SOURCES ExpressionTest.cpp
    OBJECTS ${FILTER_TEST_LIBS}
    LIBRARIES gtest gtest_main ${THRIFT_LIBRARIES} wangle
)

if (ENABLE_TESTING)
    target_compile_options(expression_test PRIVATE -Wno-parentheses)
endif()


nebula_add_executable(
    NAME expression_encode_decode_bm
    SOURCES ExpressionEncodeDecodeBenchmark.cpp
    OBJECTS ${FILTER_TEST_LIBS}
    LIBRARIES follybenchmark boost_regex ${THRIFT_LIBRARIES} wangle
)
