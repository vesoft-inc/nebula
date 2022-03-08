nebula_add_library(
    raftex_obj SHARED
    RaftLogIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
)

target_link_libraries(raftex_obj PUBLIC "${Glog_LIBRARY}" base_obj wal_obj kv_stats_obj thrift_obj stats_obj  ssl_obj fmt)
nebula_add_subdirectory(test)
