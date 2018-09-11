add_custom_target(
    tgt_fbthrift
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
    DEPENDS tgt_glog tgt_gflags tgt_folly tgt_wangle tgt_fatal tgt_mstch tgt_zlib tgt_zstd
)

add_custom_target(
    clean-fbthrift
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

