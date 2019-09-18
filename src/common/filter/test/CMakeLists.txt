set(FILTER_TEST_LIBS
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
)


nebula_add_test(
    NAME expression_test
    SOURCES ExpressionTest.cpp
    OBJECTS ${FILTER_TEST_LIBS}
    LIBRARIES gtest gtest_main
)

if (ENABLE_TESTING)
    target_compile_options(expression_test PRIVATE -Wno-parentheses)
endif()


nebula_add_executable(
    NAME expression_encode_decode_bm
    SOURCES ExpressionEncodeDecodeBenchmark.cpp
    OBJECTS ${FILTER_TEST_LIBS}
    LIBRARIES follybenchmark boost_regex
)
