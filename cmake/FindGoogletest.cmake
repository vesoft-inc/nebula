# - Try to find Googletest includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Googletest)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Googletest_FOUND            System has Googletest, include and lib dirs found
#  Googletest_INCLUDE_DIR      The Googletest includes directories.
#  Googletest_LIBRARY          The Googletest library.

find_path(Googletest_INCLUDE_DIR NAMES gmock gtest)
find_library(Googletest_LIBRARY NAMES libgmock.a libgmock_main.a libgtest.a libgtest_main.a)

if(Googletest_INCLUDE_DIR AND Googletest_LIBRARY)
    set(Googletest_FOUND TRUE)
    mark_as_advanced(
        Googletest_INCLUDE_DIR
        Googletest_LIBRARY
    )
endif()

if(NOT Googletest_FOUND)
    message(FATAL_ERROR "Googletest doesn't exist")
endif()

