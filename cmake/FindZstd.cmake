# - Try to find Zstd includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Zstd)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Zstd_FOUND            System has Zstd, include and lib dirs found
#  Zstd_INCLUDE_DIR      The Zstd includes directories.
#  Zstd_LIBRARY          The Zstd library.
#  Zstd_BIN              The Zstd binary.

find_path(Zstd_INCLUDE_DIR NAMES zstd.h)
find_library(Zstd_LIBRARY NAMES libzstd.a)

if(Zstd_INCLUDE_DIR AND Zstd_LIBRARY)
    set(Zstd_FOUND TRUE)
    mark_as_advanced(
        Zstd_INCLUDE_DIR
        Zstd_LIBRARY
    )
endif()

if(NOT Zstd_FOUND)
    message(FATAL_ERROR "Zstd doesn't exist")
endif()


