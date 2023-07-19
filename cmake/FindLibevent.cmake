# - Try to find Libevent includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(Libevent)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  Libevent_FOUND            System has Libevent, include and lib dirs found
#  Libevent_INCLUDE_DIR      The Libevent includes directories.
#  Libevent_LIBRARY          The Libevent library.

find_path(Libevent_INCLUDE_DIR NAMES event.h)
find_library(Libevent_LIBRARY NAMES libevent.a)

if(Libevent_INCLUDE_DIR AND Libevent_LIBRARY)
    set(Libevent_FOUND TRUE)
    mark_as_advanced(
        Libevent_INCLUDE_DIR
        Libevent_LIBRARY
    )
endif()

if(NOT Libevent_FOUND)
    message(FATAL_ERROR "Libevent doesn't exist")
endif()

