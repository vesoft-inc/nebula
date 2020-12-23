nebula_add_library(
    wal_obj OBJECT
    FileBasedWal.cpp
    WalFileIterator.cpp
)

nebula_add_subdirectory(test)
