build_external(tgt_jemalloc)

add_custom_target(
    clean-jemalloc
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

