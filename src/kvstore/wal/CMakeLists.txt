nebula_add_library(
    wal_obj OBJECT
    InMemoryLogBuffer.cpp
    FileBasedWal.cpp
    WalFileIterator.cpp
)

nebula_add_subdirectory(test)
