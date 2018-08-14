add_custom_target(
    tgt_double-conversion
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-double-conversion
    COMMAND "rm" "-fr" "_build" "_build.log"
)

