nebula_add_all_library(
    raftex
    LogStrListIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
)

nebula_add_subdirectory(test)
