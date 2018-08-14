add_custom_target(
    tgt_fatal
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-fatal
    COMMAND "rm" "-fr" "_build" "_build.log"
)

