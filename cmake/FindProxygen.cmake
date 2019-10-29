# - Try to find Proxygen includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Proxygen)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Proxygen_FOUND            System has Proxygen, include and lib dirs found
#  Proxygen_INCLUDE_DIR      The Proxygen includes directories.
#  Proxygen_LIBRARY          The Proxygen library.

find_path(Proxygen_INCLUDE_DIR NAMES proxygen)
find_library(Proxygen_LIBRARY NAMES libproxygen.a)

if(Proxygen_INCLUDE_DIR AND Proxygen_LIBRARY)
    set(Proxygen_FOUND TRUE)
    mark_as_advanced(
        Proxygen_INCLUDE_DIR
        Proxygen_LIBRARY
    )
endif(Proxygen_INCLUDE_DIR AND Proxygen_LIBRARY)

if(NOT Proxygen_FOUND)
    message(FATAL_ERROR "Proxygen doesn't exist")
endif()


