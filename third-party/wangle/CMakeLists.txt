add_custom_target(
    tgt_wangle
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
    DEPENDS tgt_folly tgt_libevent tgt_gflags tgt_glog tgt_double-conversion
)

add_custom_target(
    clean-wangle
    COMMAND "rm" "-fr" "_build" "_build.log"
)

