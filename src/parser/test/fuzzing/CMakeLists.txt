nebula_add_test(
    NAME parser_fuzzer
    FUZZER ON
    SOURCES ParserFuzzer.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle
)
