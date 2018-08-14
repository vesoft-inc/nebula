add_custom_target(
    tgt_zlib
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-zlib
    COMMAND "rm" "-fr" "_build" "_build.log"
)

