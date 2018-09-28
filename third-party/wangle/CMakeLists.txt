build_external(
    tgt_wangle
    tgt_folly tgt_libevent tgt_gflags tgt_glog tgt_double-conversion
)

add_custom_target(
    clean-wangle
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

