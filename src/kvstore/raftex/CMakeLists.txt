add_library(
    raftex_obj OBJECT
    LogStrListIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
)
add_dependencies(raftex_obj base_obj)

add_subdirectory(test)

