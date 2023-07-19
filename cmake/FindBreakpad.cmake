# - Try to find Breakpad includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Breakpad)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Breakpad_FOUND            System has breakpad, include and lib dirs found
#  Breakpad_INCLUDE_DIR      The breakpad includes directories.
#  Breakpad_LIBRARY          The breakpad library.

find_path(Breakpad_INCLUDE_DIR NAMES breakpad)
find_library(Breakpad_LIBRARY NAMES libbreakpad_client.a)

message(STATUS "BREAKPAD: ${Breakpad_INCLUDE_DIR}")

if(Breakpad_INCLUDE_DIR AND Breakpad_LIBRARY)
    set(Breakpad_FOUND TRUE)
    mark_as_advanced(
        Breakpad_INCLUDE_DIR
        Breakpad_LIBRARY
    )
endif()

if(NOT Breakpad_FOUND)
    message(FATAL_ERROR "Breakpad doesn't exist")
endif()


