add_library(
    meta_obj OBJECT
    PartManager.cpp
    HostManager.cpp
    SchemaManager.cpp
)
add_dependencies(meta_obj common)

#add_subdirectory(test)

