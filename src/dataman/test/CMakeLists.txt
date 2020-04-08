set(DATAMAN_TEST_LIBS
    common_thrift_lib
    thrift_lib
    dataman_lib
    base_lib
    network_lib
    fs_lib
    thread_lib
)


nebula_add_test(
    NAME
        row_reader_test
    SOURCES
        RowReaderTest.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        row_writer_test
    SOURCES
        RowWriterTest.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        row_updater_test
    SOURCES
        RowUpdaterTest.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_test(
    NAME
        rowset_reader_writer_test
    SOURCES
        RowSetReaderWriterTest.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)


nebula_add_executable(
    NAME
        row_writer_bm
    SOURCES
        RowWriterBenchmark.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
)


nebula_add_executable(
    NAME
        row_reader_bm
    SOURCES
        RowReaderBenchmark.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        follybenchmark
        wangle
        boost_regex
)

nebula_add_test(
    NAME
        nebula_codec_test
    SOURCES
        NebulaCodecTest.cpp
    LIBRARIES
        ${DATAMAN_TEST_LIBS}
        ${THRIFT_LIBRARIES}
        wangle
        gtest
)
