nebula_add_library(
    wal_obj SHARED
    FileBasedWal.cpp
    WalFileIterator.cpp
    AtomicLogBuffer.cpp
)

target_link_libraries(wal_obj PUBLIC  disk_man_obj time_obj)
nebula_add_subdirectory(test)
