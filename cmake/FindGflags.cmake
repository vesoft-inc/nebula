# - Try to find Gflags includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Gflags)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Gflags_FOUND            System has Gflags, include and lib dirs found
#  Gflags_INCLUDE_DIR      The Gflags includes directories.
#  Gflags_LIBRARY          The Gflags library.

find_path(Gflags_INCLUDE_DIR NAMES gflags)
find_library(Gflags_LIBRARY NAMES libgflags.a)

if(Gflags_INCLUDE_DIR AND Gflags_LIBRARY)
    set(Gflags_FOUND TRUE)
    mark_as_advanced(
        Gflags_INCLUDE_DIR
        Gflags_LIBRARY
    )
endif()

if(NOT Gflags_FOUND)
    message(FATAL_ERROR "Gflags doesn't exist")
endif()

