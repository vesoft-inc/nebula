build_external(tgt_libevent)

add_custom_target(
    clean-libevent
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

