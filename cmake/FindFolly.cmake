# - Try to find Folly includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Folly)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Folly_FOUND            System has folly, include and lib dirs found
#  Folly_INCLUDE_DIR      The folly includes directories.
#  Folly_LIBRARY          The folly library.

find_path(Folly_INCLUDE_DIR NAMES folly)
find_library(Folly_LIBRARY NAMES libfolly.a libfollybenchmark.a)

if(Folly_INCLUDE_DIR AND Folly_LIBRARY)
    set(Folly_FOUND TRUE)
    mark_as_advanced(
        Folly_INCLUDE_DIR
        Folly_LIBRARY
    )
endif()

if(NOT Folly_FOUND)
    message(FATAL_ERROR "Folly doesn't exist")
endif()

