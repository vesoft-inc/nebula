nebula_add_library(
    wal_obj SHARED
    FileBasedWal.cpp
    WalFileIterator.cpp
    AtomicLogBuffer.cpp
)

target_link_libraries(wal_obj PUBLIC "${Glog_LIBRARY}"  )
nebula_add_subdirectory(test)
