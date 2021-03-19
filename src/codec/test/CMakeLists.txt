nebula_add_library(
    codec_test_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
)

set(CODEC_TEST_LIBS
    $<TARGET_OBJECTS:codec_obj>
    $<TARGET_OBJECTS:codec_test_obj>
    $<TARGET_OBJECTS:common_datatypes_obj>
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:common_meta_thrift_obj>
    $<TARGET_OBJECTS:common_common_thrift_obj>
    $<TARGET_OBJECTS:common_raftex_thrift_obj>
    $<TARGET_OBJECTS:common_meta_client_obj>
    $<TARGET_OBJECTS:common_ws_obj>
    $<TARGET_OBJECTS:common_ws_common_obj>
    $<TARGET_OBJECTS:common_http_client_obj>
    $<TARGET_OBJECTS:common_file_based_cluster_id_man_obj>
    $<TARGET_OBJECTS:common_base_obj>
    $<TARGET_OBJECTS:common_network_obj>
    $<TARGET_OBJECTS:common_fs_obj>
    $<TARGET_OBJECTS:common_thread_obj>
    $<TARGET_OBJECTS:common_stats_obj>
    $<TARGET_OBJECTS:common_time_obj>
    $<TARGET_OBJECTS:common_conf_obj>
    $<TARGET_OBJECTS:common_meta_obj>
    $<TARGET_OBJECTS:common_process_obj>
    $<TARGET_OBJECTS:common_expression_obj>
    $<TARGET_OBJECTS:common_function_manager_obj>
    $<TARGET_OBJECTS:common_agg_function_manager_obj>
    $<TARGET_OBJECTS:common_time_utils_obj>
    $<TARGET_OBJECTS:common_version_obj>
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
