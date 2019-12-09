
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

# add_subdirectory(test)
