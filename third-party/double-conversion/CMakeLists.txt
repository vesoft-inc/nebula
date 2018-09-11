add_custom_target(
    tgt_double-conversion
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-double-conversion
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

