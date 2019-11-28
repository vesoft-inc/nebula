find_program(Autoconf_EXECUTABLE NAMES autoconf DOC "Path to the autconf executable")
if (Autoconf_EXECUTABLE)
    execute_process(
        OUTPUT_VARIABLE Autoconf_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND "autoconf" "--version"
        COMMAND "head" "-1"
        COMMAND "cut" "-d" " " "-f4"
    )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Autoconf REQUIRED_VARS  Autoconf_EXECUTABLE
                                  VERSION_VAR Autoconf_VERSION)
