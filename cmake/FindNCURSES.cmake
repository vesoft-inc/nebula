# - Try to find ncurses include dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(NCURSES)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  NCURSES_ROOT_DIR          Set this variable to the root installation of
#                            ncurses if the module has problems finding the
#                            proper installation path.
#
# Variables defined by this module:
#
#  NCURSES_FOUND             ncurses is found, include and lib dir
#  NCURSES_INCLUDE_DIR       The ncurses include directory
#  NCURSES_LIBRARY           The ncurses library

find_path(
    NCURSES_ROOT_DIR
    NAMES include/ncurses/curses.h
)

find_path(
    NCURSES_INCLUDE_DIR
    NAMES ncurses.h curses.h ncurses/curses.h
    HINTS ${NCURSES_ROOT_DIR}/include
)

find_library(
    NCURSES_LIBRARY
    NAMES ncurses
    HINTS ${NCURSES_ROOT_DIR}/lib
)

if(NCURSES_INCLUDE_DIR AND NCURSES_LIBRARY_DIR)
  set(NCURSES_FOUND TRUE)
else()
  FIND_LIBRARY(NCURSES_LIBRARY NAMES ncurses)
  include(FindPackageHandleStandardArgs)
  FIND_PACKAGE_HANDLE_STANDARD_ARGS(ncurses DEFAULT_MSG NCURSES_INCLUDE_DIR NCURSES_LIBRARY)
endif()

mark_as_advanced(
    NCURSES_ROOT_DIR
    NCURSES_INCLUDE_DIR
    NCURSES_LIBRARY
)
