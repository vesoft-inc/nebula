add_library(
    meta_obj OBJECT
    SchemaProviderIf.cpp
    PartManager.cpp
    FileBasedPartManager.cpp
    ServerBasedPartManager.cpp
    HostManager.cpp
    SchemaManager.cpp
    FileBasedSchemaManager.cpp
    ServerBasedSchemaManager.cpp
    AdHocSchemaManager.cpp
)
add_dependencies(meta_obj base_obj dataman_obj)

add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
)
add_dependencies(
    meta_service_handler
    base_obj
    meta_thrift_obj
    kvstore_obj
)

add_subdirectory(test)
