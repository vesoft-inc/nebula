add_custom_target(
    tgt_libevent
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-libevent
    COMMAND "rm" "-fr" "_build" "_build.log"
)

