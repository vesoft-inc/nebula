add_custom_target(
    tgt_glog
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
    DEPENDS tgt_gflags tgt_libevent tgt_double-conversion
)

add_custom_target(
    clean-glog
    COMMAND "rm" "-fr" "_build" "_build.log"
)

