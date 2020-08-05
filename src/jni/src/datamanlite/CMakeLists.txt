
add_library(nebula_codec_obj
            OBJECT
            ${NEBULA_HOME}/src/common/base/Cord.cpp
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
