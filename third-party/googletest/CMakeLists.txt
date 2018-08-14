add_custom_target(
    tgt_googletest
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
    DEPENDS tgt_gflags tgt_glog
)

add_custom_target(
    clean-googletest
    COMMAND "rm" "-fr" "_build" "_build.log"
)

