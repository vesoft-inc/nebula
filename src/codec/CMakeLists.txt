nebula_add_library(
    codec_obj OBJECT
    RowReader.cpp
    RowReaderV1.cpp
    RowReaderV2.cpp
    RowWriterV2.cpp
    RowReaderWrapper.cpp
)

nebula_add_subdirectory(test)
