set(PARSER_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
)

nebula_add_test(
    NAME parser_test
    SOURCES ParserTest.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main
)

nebula_add_test(
    NAME scanner_test
    SOURCES ScannerTest.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES gtest gtest_main
)

nebula_add_executable(
    NAME parser_benchmark
    SOURCES ParserBenchmark.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES follybenchmark boost_regex
)
