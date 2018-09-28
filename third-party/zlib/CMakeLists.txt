build_external(tgt_zlib)

add_custom_target(
    clean-zlib
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

