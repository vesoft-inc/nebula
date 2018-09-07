add_custom_target(
    tgt_bzip2
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-bzip2
    COMMAND "rm" "-fr" "_build" "_build.log"
)

