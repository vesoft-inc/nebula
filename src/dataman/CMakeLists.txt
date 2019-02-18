add_compile_options(-w -fPIC)

add_library(
    dataman_obj OBJECT
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
    NebulaCodecImpl.cpp
)
add_dependencies(dataman_obj base_obj graph_thrift_obj)

add_subdirectory(test)

add_library(
    dataman SHARED
    ResultSchemaProvider.cpp
    SchemaWriter.cpp
    RowSetReader.cpp
    RowSetWriter.cpp
    RowSetReader.cpp
    RowUpdater.cpp
    RowWriter.cpp
    RowReader.cpp
    NebulaCodecImpl.cpp
    $<TARGET_OBJECTS:base_obj>
    )

add_dependencies(dataman base_obj storage_thrift_obj)
