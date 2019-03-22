add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksdbEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksdbConfigOptions.cpp
)
add_dependencies(kvstore_obj base_obj meta_client)

add_subdirectory(test)

