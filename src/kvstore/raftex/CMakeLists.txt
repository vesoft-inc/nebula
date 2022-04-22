nebula_add_library(
    raftex_obj OBJECT
    RaftLogIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
)

nebula_add_subdirectory(test)
