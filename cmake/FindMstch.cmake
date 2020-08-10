# - Try to find Mstch includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Mstch)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Mstch_FOUND            System has Mstch, include and lib dirs found
#  Mstch_INCLUDE_DIR      The Mstch includes directories.
#  Mstch_LIBRARY          The Mstch library.

find_path(Mstch_INCLUDE_DIR NAMES mstch)
find_library(Mstch_LIBRARY NAMES libmstch.a)

if(Mstch_INCLUDE_DIR AND Mstch_LIBRARY)
    set(Mstch_FOUND TRUE)
    mark_as_advanced(
        Mstch_INCLUDE_DIR
        Mstch_LIBRARY
    )
    message(STATUS "Mstch_INCLUDE_DIR       : ${Mstch_INCLUDE_DIR}")
    message(STATUS "Mstch_LIBRARY           : ${Mstch_LIBRARY}")
endif()

if(NOT Mstch_FOUND)
    message(FATAL_ERROR "Mstch doesn't exist")
endif()

