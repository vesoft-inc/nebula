nebula_add_library(
    wal_obj OBJECT
    FileBasedWalIterator.cpp
    FileBasedWal.cpp
)

nebula_add_subdirectory(test)
