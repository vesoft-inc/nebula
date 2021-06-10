nebula_add_library(
    kvstore_obj OBJECT
    Part.cpp
    Listener.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    LogEncoder.cpp
    SnapshotManagerImpl.cpp
    plugins/elasticsearch/ESListener.cpp
)

nebula_add_library(
    disk_man_obj OBJECT
    DiskManager.cpp
)

nebula_add_subdirectory(raftex)
nebula_add_subdirectory(wal)
nebula_add_subdirectory(test)

#nebula_add_subdirectory(plugins)

