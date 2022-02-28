nebula_add_library(
    codec_test_obj SHARED
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
)
target_link_libraries(codec_test_obj PUBLIC "${Glog_LIBRARY}"  )

set(CODEC_TEST_LIBS
    codec_obj
    codec_test_obj
    datatypes_obj
    thrift_obj
    meta_thrift_obj
    common_thrift_obj
    raftex_thrift_obj
    meta_client_obj
    ws_obj
    ws_common_obj
    http_client_obj
    file_based_cluster_id_man_obj
    base_obj
    network_obj
    fs_obj
    thread_obj
    stats_obj
    meta_client_stats_obj
    storage_client_stats_obj
    kv_stats_obj
    time_obj
    conf_obj
    meta_obj
    process_obj
    expression_obj
    function_manager_obj
    wkt_wkb_io_obj
    agg_function_manager_obj
    time_utils_obj
    datetime_parser_obj
    version_obj
    ssl_obj
)


nebula_add_test(
    NAME row_reader_v1_test
    SOURCES RowReaderV1Test.cpp
    LIBRARIES ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME row_reader_v2_test
    SOURCES RowReaderV2Test.cpp
    LIBRARIES ${CODEC_TEST_LIBS}
    LIBRARIES
        ${THRIFT_LIBRARIES}
        ${PROXYGEN_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME row_writer_v2_test
    SOURCES RowWriterV2Test.cpp
    LIBRARIES ${CODEC_TEST_LIBS}
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
    LIBRARIES ${CODEC_TEST_LIBS}
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
    LIBRARIES ${CODEC_TEST_LIBS}
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
#    LIBRARIES ${CODEC_TEST_LIBS}
#    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
#)
