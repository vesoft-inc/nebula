nebula_add_library(
    kvstore_obj SHARED
    Part.cpp
    Listener.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    LogEncoder.cpp
    NebulaSnapshotManager.cpp
    RateLimiter.cpp
    plugins/elasticsearch/ESListener.cpp
)

nebula_add_library(
    disk_man_obj SHARED
    DiskManager.cpp
)
target_link_libraries(disk_man_obj PUBLIC ${Glog_LIBRARY})
target_link_libraries(kvstore_obj PUBLIC conf_obj codec_obj raftex_obj ft_es_storage_adapter_obj thread_obj keyutils_obj disk_man_obj thrift thriftcpp2)

nebula_add_subdirectory(stats)
nebula_add_subdirectory(raftex)
nebula_add_subdirectory(wal)
nebula_add_subdirectory(test)

#nebula_add_subdirectory(plugins)

