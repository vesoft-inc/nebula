add_library(
    wal_obj OBJECT
    BufferFlusher.cpp
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)
nebula_add_base_obj(wal_obj)

add_subdirectory(test)

