add_custom_target(
    tgt_zstd
    COMMAND "./build.sh" "-j16" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-zstd
    COMMAND "rm" "-fr" "_build" "_build.log"
)

