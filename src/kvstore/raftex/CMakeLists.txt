add_library(
    raftex_obj OBJECT
    LogStrListIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
)
nebula_add_base_obj(raftex_obj)

add_subdirectory(test)

