add_library(
    meta_obj OBJECT
    ShardManager.cpp
    HostManager.cpp
    SchemaManager.cpp
)
add_dependencies(meta_obj common)

#add_subdirectory(test)

