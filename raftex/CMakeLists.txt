add_library(
    raftex_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)
add_dependencies(raftex_obj common)

add_subdirectory(test)

