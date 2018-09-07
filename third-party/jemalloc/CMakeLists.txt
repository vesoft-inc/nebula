add_custom_target(
    tgt_jemalloc
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-jemalloc
    COMMAND "rm" "-fr" "_build" "_build.log"
)

