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
nebula_add_base_obj(dataman_obj)

add_subdirectory(test)
