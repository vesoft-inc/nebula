build_external(tgt_zstd)

add_custom_target(
    clean-zstd
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

