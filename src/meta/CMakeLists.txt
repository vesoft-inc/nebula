add_library(
    meta_obj OBJECT
    PartManager.cpp
    HostManager.cpp
    SchemaManager.cpp
)
add_dependencies(meta_obj common dataman_obj)

#add_subdirectory(test)

