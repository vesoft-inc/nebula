nebula_add_library(
    dataman2_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowWriter.cpp
    RowReader.cpp
)

add_subdirectory(test)
