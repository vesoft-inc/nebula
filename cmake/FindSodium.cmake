# - Try to find Sodium includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Sodium)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Sodium_FOUND            System has Sodium, include and lib dirs found
#  Sodium_INCLUDE_DIR      The Sodium includes directories.
#  Sodium_LIBRARY          The Sodium library.

find_path(Sodium_INCLUDE_DIR NAMES sodium)
find_library(Sodium_LIBRARY NAMES libsodium.a)

if(Sodium_INCLUDE_DIR AND Sodium_LIBRARY)
    set(Sodium_FOUND TRUE)
    mark_as_advanced(
        Sodium_INCLUDE_DIR
        Sodium_LIBRARY
    )
endif()

if(NOT Sodium_FOUND)
    message(FATAL_ERROR "Sodium doesn't exist")
endif()

