add_library(
    dataman_obj OBJECT
    SchemaProviderIf.cpp
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
)
add_dependencies(dataman_obj common graph_thrift_obj)

add_subdirectory(test)

