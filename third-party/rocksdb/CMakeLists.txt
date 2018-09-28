build_external(
    tgt_rocksdb
    tgt_bzip2 tgt_snappy tgt_zlib tgt_zstd tgt_jemalloc
)

add_custom_target(
    clean-rocksdb
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

