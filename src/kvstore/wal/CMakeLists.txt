add_library(
    wal_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)
add_dependencies(wal_obj base_obj)

add_subdirectory(test)

