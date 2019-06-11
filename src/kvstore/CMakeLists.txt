add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    LogEncoder.cpp
)
add_dependencies(kvstore_obj base_obj meta_client)

add_subdirectory(raftex)
add_subdirectory(wal)
add_subdirectory(test)

