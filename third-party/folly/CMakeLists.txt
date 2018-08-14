add_custom_target(
    tgt_folly
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
    DEPENDS tgt_gflags tgt_glog tgt_zlib tgt_zstd
)

add_custom_target(
    clean-folly
    COMMAND "rm" "-fr" "_build" "_build.log"
)

