find_program(Automake_EXECUTABLE NAMES automake DOC "Path to the automake executable")
if (Automake_EXECUTABLE)
    execute_process(
        OUTPUT_VARIABLE Automake_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND "automake" "--version"
        COMMAND "head" "-1"
        COMMAND "cut" "-d" " " "-f4"
    )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Automake REQUIRED_VARS  Automake_EXECUTABLE
                                  VERSION_VAR Automake_VERSION)
