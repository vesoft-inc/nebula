add_custom_target(
    tgt_proxygen
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
    DEPENDS tgt_folly tgt_wangle tgt_gflags tgt_glog
)

add_custom_target(
    clean-proxygen
    COMMAND "rm" "-fr" "_build" "_build.log"
)

