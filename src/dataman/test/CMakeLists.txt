set(DATAMAN_TEST_LIBS
    $<TARGET_OBJECTS:common_thrift_obj>
    $<TARGET_OBJECTS:thrift_obj>
    $<TARGET_OBJECTS:dataman_obj>
    $<TARGET_OBJECTS:base_obj>
    $<TARGET_OBJECTS:network_obj>
    $<TARGET_OBJECTS:fs_obj>
    $<TARGET_OBJECTS:thread_obj>
)


nebula_add_test(
    NAME row_reader_test
    SOURCES RowReaderTest.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME row_writer_test
    SOURCES RowWriterTest.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME row_updater_test
    SOURCES RowUpdaterTest.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_test(
    NAME rowset_reader_writer_test
    SOURCES RowSetReaderWriterTest.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
)


nebula_add_executable(
    NAME row_writer_bm
    SOURCES RowWriterBenchmark.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} follybenchmark wangle boost_regex
)


nebula_add_executable(
    NAME row_reader_bm
    SOURCES RowReaderBenchmark.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} follybenchmark wangle boost_regex
)

nebula_add_test(
    NAME nebula_codec_test
    SOURCES NebulaCodecTest.cpp
    OBJECTS ${DATAMAN_TEST_LIBS}
    LIBRARIES ${THRIFT_LIBRARIES} wangle gtest
)
