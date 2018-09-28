build_external(
    tgt_proxygen
    tgt_folly tgt_wangle tgt_gflags tgt_glog
)

add_custom_target(
    clean-proxygen
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

