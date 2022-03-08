nebula_add_library(
    wal_obj OBJECT
    FileBasedWal.cpp
    WalFileIterator.cpp
    AtomicLogBuffer.cpp
)

nebula_add_subdirectory(test)
