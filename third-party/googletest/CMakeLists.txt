add_custom_target(
    tgt_googletest
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
    DEPENDS tgt_gflags tgt_glog
)

add_custom_target(
    clean-googletest
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

