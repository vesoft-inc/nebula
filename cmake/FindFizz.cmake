# - Try to find Fizz includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Fizz)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Fizz_FOUND            System has fizz, include and lib dirs found
#  Fizz_INCLUDE_DIR      The fizz includes directories.
#  Fizz_LIBRARY          The fizz library.

find_path(Fizz_INCLUDE_DIR NAMES fizz)
find_library(Fizz_LIBRARY NAMES libfizz.a)

if(Fizz_INCLUDE_DIR AND Fizz_LIBRARY)
    set(Fizz_FOUND TRUE)
    mark_as_advanced(
        Fizz_INCLUDE_DIR
        Fizz_LIBRARY
    )
endif()

if(NOT Fizz_FOUND)
    message(FATAL_ERROR "Fizz doesn't exist")
endif()

