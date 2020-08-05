find_program(Libtool_EXECUTABLE NAMES libtool libtoolize DOC "Path to the libtool executable")
if (Libtool_EXECUTABLE)
    execute_process(
        OUTPUT_VARIABLE Libtool_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE
        COMMAND ${Libtool_EXECUTABLE} "--version"
        COMMAND "head" "-1"
        COMMAND "cut" "-d" " " "-f4"
    )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libtool REQUIRED_VARS  Libtool_EXECUTABLE
                                  VERSION_VAR Libtool_VERSION)
