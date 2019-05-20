add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksEngine.cpp
    PartManager.cpp
    KVStore.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    LogEncoder.cpp
)
add_dependencies(kvstore_obj base_obj meta_client hbasestore_obj)

add_subdirectory(raftex)
add_subdirectory(wal)
add_subdirectory(hbase)
add_subdirectory(test)

