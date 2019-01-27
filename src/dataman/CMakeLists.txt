add_library(
    dataman_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
)
add_dependencies(dataman_obj base_obj graph_thrift_obj)

add_subdirectory(test)

