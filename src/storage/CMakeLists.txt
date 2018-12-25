
add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    KeyUtils.cpp
    AddVerticesProcessor.cpp
)

add_dependencies(storage_service_handler common storage_thrift_obj kvstore_obj)

add_subdirectory(test)

