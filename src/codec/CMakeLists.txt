nebula_add_library(
    codec_obj SHARED
    RowReader.cpp
    RowReaderV1.cpp
    RowReaderV2.cpp
    RowWriterV2.cpp
    RowReaderWrapper.cpp
)
target_link_libraries(codec_obj PUBLIC wkt_wkb_io_obj )

nebula_add_subdirectory(test)
