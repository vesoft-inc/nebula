nebula_add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    LogEncoder.cpp
    SnapshotManagerImpl.cpp
)

add_subdirectory(raftex)
add_subdirectory(wal)
add_subdirectory(test)

add_subdirectory(plugins)

