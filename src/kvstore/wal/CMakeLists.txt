nebula_add_library(
    wal_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)

add_subdirectory(test)
