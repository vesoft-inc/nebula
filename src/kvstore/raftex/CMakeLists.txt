nebula_add_library(
    raftex_obj SHARED
    RaftLogIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
)

target_link_libraries(raftex_obj PUBLIC "${Glog_LIBRARY}" base_obj )
nebula_add_subdirectory(test)
