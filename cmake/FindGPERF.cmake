# FindGPERF
# ---------
#
# Find ``gperf`` executable
#
# The module defines the following variables:
#
# ``GPERF_EXECUTABLE_DIR``
#   path to search the gperf binary
#
# ``GPERF_EXECUTABLE``
#   path to the ``gperf`` program
#
# ``GPERF_BIN_DIR``
#   path to the directory that holds ``gperf`` program
#
# ``GPERF_VERSION``
#   version of ``gperf``
#
# ``GPERF_FOUND``
#   true if the program was found
#
# The minimum required version of ``gperf`` can be specified using the
# standard CMake syntax, e.g.  ``find_package(GPERF 3.0)``.

find_program(GPERF_EXECUTABLE
             NAMES gperf
             PATHS ${GPERF_EXECUTABLE_DIR}
             DOC "path to the gperf executable")

if(GPERF_EXECUTABLE)
  # Extract the path
  STRING(REGEX REPLACE "/gperf$" "" GPERF_BIN_DIR ${GPERF_EXECUTABLE})

  # the gperf commands should be executed with the C locale, otherwise
  # the message (which is parsed) may be translated
  set(_gperf_SAVED_LC_ALL "$ENV{LC_ALL}")
  set(ENV{LC_ALL} C)

  execute_process(COMMAND ${GPERF_EXECUTABLE} --version
    OUTPUT_VARIABLE GPERF_version_output
    ERROR_VARIABLE GPERF_version_error
    RESULT_VARIABLE GPERF_version_result
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  set(ENV{LC_ALL} ${_gperf_SAVED_LC_ALL})

  if(NOT ${GPERF_version_result} EQUAL 0)
    message(SEND_ERROR "Command \"${GPERF_EXECUTABLE} --version\" failed with output:\n${GPERF_version_error}")
  else()
    if("${GPERF_version_output}" MATCHES "^GNU gperf ([^\n]+)")
      set(GPERF_VERSION "${CMAKE_MATCH_1}")
    endif()
  endif()

endif()
mark_as_advanced(GPERF_EXECUTABLE GPERF_BIN_DIR)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GPERF REQUIRED_VARS  GPERF_EXECUTABLE
                                        VERSION_VAR GPERF_VERSION)
