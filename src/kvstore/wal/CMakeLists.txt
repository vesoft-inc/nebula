nebula_add_library(
    wal_obj OBJECT
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)

add_subdirectory(test)
