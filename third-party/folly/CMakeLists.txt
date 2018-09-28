build_external(
    tgt_folly
    tgt_gflags tgt_glog tgt_zlib tgt_zstd
)

add_custom_target(
    clean-folly
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

