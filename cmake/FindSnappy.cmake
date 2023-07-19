# - Try to find Snappy includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Snappy)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Snappy_FOUND            System has Snappy, include and lib dirs found
#  Snappy_INCLUDE_DIR      The Snappy includes directories.
#  Snappy_LIBRARY          The Snappy library.

find_path(Snappy_INCLUDE_DIR NAMES snappy.h)
find_library(Snappy_LIBRARY NAMES libsnappy.a)

if(Snappy_INCLUDE_DIR AND Snappy_LIBRARY)
    set(Snappy_FOUND TRUE)
    mark_as_advanced(
        Snappy_INCLUDE_DIR
        Snappy_LIBRARY
    )
endif()

if(NOT Snappy_FOUND)
    message(FATAL_ERROR "Snappy doesn't exist")
endif()

