# - Try to find Wangle includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Wangle)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Wangle_FOUND            System has Wangle, include and lib dirs found
#  Wangle_INCLUDE_DIR      The Wangle includes directories.
#  Wangle_LIBRARY          The Wangle library.

find_path(Wangle_INCLUDE_DIR NAMES wangle)
find_library(Wangle_LIBRARY NAMES libwangle.a)

if(Wangle_INCLUDE_DIR AND Wangle_LIBRARY)
    set(Wangle_FOUND TRUE)
    mark_as_advanced(
        Wangle_INCLUDE_DIR
        Wangle_LIBRARY
    )
endif()

if(NOT Wangle_FOUND)
    message(FATAL_ERROR "Wangle doesn't exist")
endif()

