set(PARSER_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
)

nebula_add_test(
    NAME parser_fuzzer
    SOURCES ParserFuzzer.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    FUZZER ON
)
