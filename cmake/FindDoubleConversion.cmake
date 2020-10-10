# - Try to find libdouble-conversion includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(DoubleConversion)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  DoubleConversion_FOUND            System has double-conversion, include and lib dirs found
#  DoubleConversion_INCLUDE_DIR      The double-conversion includes directories.
#  DoubleConversion_LIBRARY          The double-conversion library.

find_path(DoubleConversion_INCLUDE_DIR NAMES double-conversion/double-conversion.h)
find_library(DoubleConversion_LIBRARY NAMES libdouble-conversion.a)

if(DoubleConversion_INCLUDE_DIR AND DoubleConversion_LIBRARY)
    set(DoubleConversion_FOUND TRUE)
    mark_as_advanced(
        DoubleConversion_INCLUDE_DIR
        DoubleConversion_LIBRARY
    )
endif()

if(NOT DoubleConversion_FOUND)
    message(FATAL_ERROR "Double-conversion doesn't exist")
endif()

