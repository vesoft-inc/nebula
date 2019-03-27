add_library(
    schema_obj OBJECT
    SchemaManager.cpp
    FileBasedSchemaManager.cpp
    ServerBasedSchemaManager.cpp
    AdHocSchemaManager.cpp
)
add_dependencies(schema_obj base_obj)


add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
    MetaUtils.cpp
    MetaHttpHandler.cpp
    processors/CreateSpaceProcessor.cpp
    processors/AddHostsProcessor.cpp
    processors/ListHostsProcessor.cpp
    processors/ListSpacesProcessor.cpp
    processors/GetPartsAllocProcessor.cpp
    processors/GetProcessor.cpp
    processors/MultiGetProcessor.cpp
    processors/PutProcessor.cpp
    processors/MultiPutProcessor.cpp
    processors/RemoveProcessor.cpp
    processors/RemoveRangeProcessor.cpp
    processors/ScanProcessor.cpp
)

add_dependencies(
    meta_service_handler
    base_obj
    meta_thrift_obj
    kvstore_obj
)

add_library(
    meta_client OBJECT
    client/MetaClient.cpp
)

add_dependencies(meta_client base_obj meta_thrift_obj)

add_subdirectory(test)
