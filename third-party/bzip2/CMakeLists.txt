build_external(tgt_bzip2)

add_custom_target(
    clean-bzip2
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

