add_custom_target(
    tgt_rocksdb
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
    DEPENDS tgt_bzip2 tgt_snappy tgt_zlib tgt_zstd
)

add_custom_target(
    clean-rocksdb
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

