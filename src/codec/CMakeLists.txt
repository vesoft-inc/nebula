nebula_add_library(
    codec_obj OBJECT
    RowReader.cpp
    RowReaderV1.cpp
)

nebula_add_subdirectory(test)
