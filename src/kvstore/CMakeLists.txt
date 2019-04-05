add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
)
add_dependencies(kvstore_obj base_obj meta_client)

add_subdirectory(test)

