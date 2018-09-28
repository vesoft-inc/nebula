build_external(
    tgt_fbthrift
    tgt_glog tgt_gflags tgt_folly tgt_wangle tgt_fatal tgt_mstch tgt_zlib tgt_zstd)

add_custom_target(
    clean-fbthrift
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

