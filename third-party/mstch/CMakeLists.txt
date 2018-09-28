build_external(tgt_mstch)

add_custom_target(
    clean-mstch
    COMMAND "rm" "-fr" "_build" "_install" "_build.log"
)

