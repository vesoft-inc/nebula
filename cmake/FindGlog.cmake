# - Try to find Glog includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Glog)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Glog_FOUND            System has Glog, include and lib dirs found
#  Glog_INCLUDE_DIR      The Glog includes directories.
#  Glog_LIBRARY          The Glog library.

find_path(Glog_INCLUDE_DIR NAMES glog)
find_library(Glog_LIBRARY NAMES libglog.a)

if(Glog_INCLUDE_DIR AND Glog_LIBRARY)
    set(Glog_FOUND TRUE)
    mark_as_advanced(
        Glog_INCLUDE_DIR
        Glog_LIBRARY
    )
endif()

if(NOT Glog_FOUND)
    message(FATAL_ERROR "Glog doesn't exist")
endif()

