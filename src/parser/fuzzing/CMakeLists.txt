set(PARSER_TEST_LIBS
    $<TARGET_OBJECTS:parser_obj>
    $<TARGET_OBJECTS:filter_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:gflags_man_obj>
    $<TARGET_OBJECTS:schema_obj>
    $<TARGET_OBJECTS:meta_client>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:storage_client>
    $<TARGET_OBJECTS:storage_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:base_obj>
)

nebula_add_test(
    NAME parser_fuzzer
    SOURCES ParserFuzzer.cpp
    OBJECTS ${PARSER_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle
    FUZZER ON
)
