add_library(
    meta_obj OBJECT
    PartManager.cpp
    HostManager.cpp
    SchemaManager.cpp
)
add_dependencies(meta_obj common)

add_library(
    meta_service_handler OBJECT
    MetaServiceHandler.cpp
)

add_dependencies(meta_service_handler common meta_thrift_obj kvstore_obj)

#add_subdirectory(test)
