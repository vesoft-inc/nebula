add_executable(
    row_reader_test
    RowReaderTest.cpp
)

target_link_libraries(
    row_reader_test
    $<TARGET_OBJECTS:nebula_codec_obj>
    folly
    double-conversion
    gtest
    glog
    gflags
    unwind
    pthread
)

add_test(NAME row_reader_test COMMAND row_reader_test)

add_executable(
    row_writer_test
    RowWriterTest.cpp
)

target_link_libraries(
    row_writer_test
    $<TARGET_OBJECTS:nebula_codec_obj>
    folly
    double-conversion
    gtest
    glog
    gflags
    unwind
    pthread
    dl
)

add_test(NAME row_writer_test COMMAND row_writer_test)
