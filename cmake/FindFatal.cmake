# - Try to find Fatal includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Fatal)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Fatal_FOUND            System has readline, include and lib dirs found
#  Fatal_INCLUDE_DIR      The readline includes directories.

find_path(Fatal_INCLUDE_DIR NAMES fatal)

if(Fatal_INCLUDE_DIR)
    set(Fatal_FOUND TRUE)
    mark_as_advanced(
        Fatal_INCLUDE_DIR
    )
endif()

if(NOT Fatal_FOUND)
    message(FATAL_ERROR "Fatal doesn't exist")
endif()

