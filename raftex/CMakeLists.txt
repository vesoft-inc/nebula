add_library(
    raftex_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
    LogStrListIterator.cpp
    RaftPart.cpp
    Host.cpp
    RaftexService.cpp
)
add_dependencies(raftex_obj common)

add_subdirectory(test)

