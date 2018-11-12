
add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    KeyUtils.cpp
    AddVerticesProcessor.cpp
    AddEdgesProcessor.cpp
    QueryBoundProcessor.cpp
)

add_dependencies(storage_service_handler common storage_thrift_obj kvstore_obj dataman_obj meta_obj)

add_subdirectory(test)

