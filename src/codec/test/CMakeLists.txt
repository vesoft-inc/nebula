nebula_add_library(
    codec_test_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
)

set(CODEC_TEST_LIBS
    $<TARGET_OBJECTS:codec_obj>
    $<TARGET_OBJECTS:codec_test_obj>
    $<TARGET_OBJECTS:datatypes_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:meta_thrift_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:raftex_thrift_obj>
    $<TARGET_OBJECTS:meta_client_obj>
    $<TARGET_OBJECTS:ws_obj>
    $<TARGET_OBJECTS:ws_common_obj>
    $<TARGET_OBJECTS:http_client_obj>
    $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:thread_obj>
    $<TARGET_OBJECTS:stats_obj>
    $<TARGET_OBJECTS:time_obj>
    $<TARGET_OBJECTS:conf_obj>
    $<TARGET_OBJECTS:meta_obj>
    $<TARGET_OBJECTS:process_obj>
    $<TARGET_OBJECTS:expression_obj>
    $<TARGET_OBJECTS:function_manager_obj>
    $<TARGET_OBJECTS:wkt_wkb_io_obj>
    $<TARGET_OBJECTS:agg_function_manager_obj>
    $<TARGET_OBJECTS:time_utils_obj>
    $<TARGET_OBJECTS:version_obj>
    $<TARGET_OBJECTS:ssl_obj>
)


nebula_add_test(
    NAME row_reader_v1_test
    SOURCES RowReaderV1Test.cpp
    OBJECTS ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME row_reader_v2_test
    SOURCES RowReaderV2Test.cpp
    OBJECTS ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME row_writer_v2_test
    SOURCES RowWriterV2Test.cpp
    OBJECTS ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME row_reader_bm
    SOURCES
        RowReaderBenchmark.cpp
        RowWriterV1.cpp
    OBJECTS ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
        gtest
)


nebula_add_executable(
    NAME row_writer_bm
    SOURCES
        RowWriterBenchmark.cpp
        RowWriterV1.cpp
    OBJECTS ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
)

#nebula_add_test(
#    NAME nebula_codec_test
#    SOURCES NebulaCodecTest.cpp
#    OBJECTS ${CODEC_TEST_LIBS}
#    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
#)
