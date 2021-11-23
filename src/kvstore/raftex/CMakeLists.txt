nebula_add_library(
    raftex_obj OBJECT
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
)

nebula_add_subdirectory(test)
