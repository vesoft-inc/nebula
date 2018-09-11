set(ADDITIONAL_MAKE_CLEAN_FILES "_build/")

add_subdirectory(bzip2)
add_subdirectory(double-conversion)
add_subdirectory(fatal)
add_subdirectory(fbthrift)
add_subdirectory(folly)
add_subdirectory(gflags)
add_subdirectory(glog)
add_subdirectory(googletest)
add_subdirectory(jemalloc)
add_subdirectory(libevent)
add_subdirectory(mstch)
add_subdirectory(proxygen)
add_subdirectory(rocksdb)
add_subdirectory(snappy)
add_subdirectory(wangle)
add_subdirectory(zlib)
add_subdirectory(zstd)

add_custom_target(
    third-party ALL
    DEPENDS tgt_fbthrift tgt_rocksdb tgt_proxygen tgt_googletest tgt_jemalloc
)

add_custom_target(
    clean-third-party
    DEPENDS clean-bzip2 clean-double-conversion clean-fatal clean-fbthrift clean-folly clean-gflags clean-glog clean-googletest clean-jemalloc clean-libevent clean-mstch clean-proxygen clean-rocksdb clean-snappy clean-wangle clean-zlib clean-zstd
)
