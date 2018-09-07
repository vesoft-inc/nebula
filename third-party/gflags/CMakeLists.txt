add_custom_target(
    tgt_gflags
    COMMAND "./build.sh" "-j2" ">" "./_build.log" "2>&1"
)

add_custom_target(
    clean-gflags
    COMMAND "rm" "-fr" "_build" "_build.log"
)

