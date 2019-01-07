add_library(
    wal_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)
add_dependencies(wal_obj common)

add_subdirectory(test)

