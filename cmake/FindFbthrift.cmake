# - Try to find Fbthrift includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Fbthrift)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Fbthrift_FOUND            System has fbthrift, thrift1 and include and lib dirs found.
#  Fbthrift_INCLUDE_DIR      The fbthrift includes directories.
#  Fbthrift_LIBRARY          The fbthrift library.
#  Fbthrift_BIN              The fbthrift binary.

find_path(Fbthrift_INCLUDE_DIR NAMES thrift)
find_library(Fbthrift_LIBRARY NAMES libthriftcpp2.a)
find_program(Fbthrift_BIN NAMES thrift1)

if(Fbthrift_INCLUDE_DIR AND Fbthrift_LIBRARY AND Fbthrift_BIN)
    set(Fbthrift_FOUND TRUE)
    mark_as_advanced(
        Fbthrift_INCLUDE_DIR
        Fbthrift_LIBRARY
        Fbthrift_BIN
    )
endif()

if(NOT Fbthrift_FOUND)
    message(FATAL_ERROR "Fbthrift doesn't exist")
endif()

