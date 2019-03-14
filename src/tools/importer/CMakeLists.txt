add_custom_target(
    importer ALL
    COMMAND "./build.sh" ">" "./_build.log" "2>&1"
    DEPENDS java-client
)
