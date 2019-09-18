# - Try to find Jemalloc includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Jemalloc)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Jemalloc_FOUND            System has Jemalloc, include and lib dirs found
#  Jemalloc_INCLUDE_DIR      The Jemalloc includes directories.
#  Jemalloc_LIBRARY          The Jemalloc library.

find_path(Jemalloc_INCLUDE_DIR NAMES jemalloc)
find_library(Jemalloc_LIBRARY NAMES libjemalloc.a)

if(Jemalloc_INCLUDE_DIR AND Jemalloc_LIBRARY)
    set(Jemalloc_FOUND TRUE)
    mark_as_advanced(
        Jemalloc_INCLUDE_DIR
        Jemalloc_LIBRARY
    )
endif()

if(NOT Jemalloc_FOUND)
    message(FATAL_ERROR "Jemalloc doesn't exist")
endif()
