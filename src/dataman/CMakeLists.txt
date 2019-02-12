set(CMAKE_CXX_FLAGS "-w")

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
    ../common/base/Cord.cpp
    ../common/base/Configuration.cpp
    ../common/base/Status.cpp
    ../meta/SchemaProviderIf.cpp
    ../meta/SchemaManager.cpp
    ../meta/FileBasedSchemaManager.cpp
    ../meta/ServerBasedSchemaManager.cpp
    ../meta/AdHocSchemaManager.cpp
    ../interface/gen-cpp2/storage_constants.cpp
    ../interface/gen-cpp2/storage_data.cpp
    ../interface/gen-cpp2/storage_types.cpp
)

add_dependencies(dataman base_obj storage_thrift_obj)
