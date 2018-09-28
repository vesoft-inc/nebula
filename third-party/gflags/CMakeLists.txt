build_external(tgt_gflags)

add_custom_target(
    clean-gflags
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

