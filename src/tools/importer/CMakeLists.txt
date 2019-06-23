get_property(java-fbthrift-jar GLOBAL PROPERTY java-fbthrift-jar)
add_custom_target(
    importer ALL
    COMMAND "./build.sh" ${java-fbthrift-jar} ">" "./_build.log" "2>&1"
    DEPENDS java-client
)
