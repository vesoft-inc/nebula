nebula_add_library(
    wal_obj OBJECT
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)

nebula_add_subdirectory(test)
