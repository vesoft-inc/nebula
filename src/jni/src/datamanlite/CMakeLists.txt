
add_library(nebula_codec_obj
            OBJECT
            ${CMAKE_CURRENT_SOURCE_DIR}/../../../common/base/Cord.cpp
            Slice.cpp
            DataCommon.cpp
            SchemaProviderIf.cpp
            NebulaSchemaProvider.cpp
            RowReader.cpp
            RowWriter.cpp
           )

if (ENABLE_TESTING)
add_subdirectory(test)
endif()
