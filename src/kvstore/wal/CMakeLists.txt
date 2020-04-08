nebula_add_all_library(
    wal
    InMemoryLogBuffer.cpp
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)

nebula_add_subdirectory(test)
