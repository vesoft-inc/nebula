# - Try to find Bzip2 includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Bzip2)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Bzip2_FOUND            System has bzip2, include and lib dirs found.
#  Bzip2_INCLUDE_DIR      The bzip2 includes directories.
#  Bzip2_LIBRARY          The bzip2 library.
#  Bzip2_BIN              The bzip2 binary.

find_path(Bzip2_INCLUDE_DIR NAMES bzlib.h)
find_library(Bzip2_LIBRARY NAMES libbz2.a)
find_program(Bzip2_BIN NAMES bzip2)

if(Bzip2_INCLUDE_DIR AND Bzip2_LIBRARY AND Bzip2_BIN)
    set(Bzip2_FOUND TRUE)
    mark_as_advanced(
        Bzip2_INCLUDE_DIR
        Bzip2_LIBRARY
        Bzip2_BIN
    )
endif()

if(NOT Bzip2_FOUND)
    message(FATAL_ERROR "Bzip2 doesn't exist")
endif()


