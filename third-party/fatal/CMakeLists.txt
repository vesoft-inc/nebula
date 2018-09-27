build_external(tgt_fatal)

add_custom_target(
    clean-fatal
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

