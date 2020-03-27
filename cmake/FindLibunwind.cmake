# FindLibunwind
# -------------
#
# Find Libunwind
#
# Find LibUnwind library
#
# ::
#
#   LIBUNWIND_LIBRARY_DIR       - Can be provided to advise the search
#
#   Libunwind_FOUND             - True if libunwind is found.
#   LIBUNWIND_LIBRARIES         - libunwind libraries to link against.

find_library(LIBUNWIND_LIBRARY NAMES unwind libunwind.so.8 PATHS ${LIBUNWIND_LIBRARY_DIR})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Libunwind  REQUIRED_VARS  LIBUNWIND_LIBRARY)

if (Libunwind_FOUND)
    set(LIBUNWIND_LIBRARIES ${LIBUNWIND_LIBRARY})
endif ()

mark_as_advanced( LIBUNWIND_LIBRARY )
