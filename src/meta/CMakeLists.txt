add_library(
    meta_obj OBJECT
    SchemaProviderIf.cpp
    PartManager.cpp
    FileBasedPartManager.cpp
    HostManager.cpp
    SchemaManager.cpp
    FileBasedSchemaManager.cpp
)
add_dependencies(meta_obj common dataman_obj)

add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
)
add_dependencies(
    meta_service_handler
    common
    meta_thrift_obj
    kvstore_obj
)

add_subdirectory(test)
