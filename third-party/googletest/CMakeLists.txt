build_external(
    tgt_googletest
    tgt_gflags tgt_glog
)

add_custom_target(
    clean-googletest
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

