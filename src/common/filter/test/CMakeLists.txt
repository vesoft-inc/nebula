set(FILTER_TEST_LIBS
    filter_lib
    storage_client_lib
    meta_client_lib
    stats_lib
    time_lib
    schema_lib
    storage_thrift_lib
    meta_thrift_lib
    common_thrift_lib
    thrift_lib
    gflags_man_lib
    parser_lib
    network_lib
    fs_lib
    thread_lib
    time_lib
    base_lib
)


nebula_add_test(
    NAME
        expression_test
    SOURCES
        ExpressionTest.cpp
    LIBRARIES
        ${FILTER_TEST_LIBS}
        gtest
        gtest_main
        ${THRIFT_LIBRARIES}
        wangle
)

if (ENABLE_TESTING)
    target_compile_options(expression_test PRIVATE -Wno-parentheses)
endif()


nebula_add_executable(
    NAME
        expression_encode_decode_bm
    SOURCES
        ExpressionEncodeDecodeBenchmark.cpp
    LIBRARIES
        ${FILTER_TEST_LIBS}
        follybenchmark
        boost_regex
        ${THRIFT_LIBRARIES}
        wangle
)
