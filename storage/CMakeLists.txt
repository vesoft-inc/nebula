add_library(
    storage_obj OBJECT
    Shard.cpp
    RocksdbEngine.cpp
)
add_dependencies(storage_obj common)

add_subdirectory(test)

