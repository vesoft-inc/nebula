# - Try to find CacheLib includes dirs and libraries
#
# Usage of this module as follows:
#
#     find_package(CacheLib)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
# Variables defined by this module:
#
#  CacheLib_FOUND            System has CacheLib, include and lib dirs found
#  CacheLib_INCLUDE_DIR      The CacheLib includes directories.
#  CacheLib_LIBRARIES        The CacheLib libraries.

find_path(CacheLib_INCLUDE_DIR NAMES cachelib)
find_library(CacheLib_ALLOCATOR NAMES libcachelib_allocator.a)
find_library(CacheLib_DATATYPE NAMES libcachelib_datatype.a)
find_library(CacheLib_SHM NAMES libcachelib_shm.a)
find_library(CacheLib_COMMON NAMES libcachelib_common.a)
find_library(CacheLib_NAVY NAMES libcachelib_navy.a)


if(CacheLib_INCLUDE_DIR
    AND CacheLib_ALLOCATOR
    AND CacheLib_DATATYPE
    AND CacheLib_SHM
    AND CacheLib_COMMON
    AND CacheLib_NAVY)
        set(CacheLib_FOUND TRUE)
        set(CacheLib_LIBRARIES ${CacheLib_DATATYPE} ${CacheLib_COMMON} ${CacheLib_SHM} ${CacheLib_ALLOCATOR} ${CacheLib_NAVY})
        mark_as_advanced(
            CacheLib_INCLUDE_DIR
            CacheLib_LIBRARIES
        )
endif()

if(NOT CacheLib_FOUND)
    message(FATAL_ERROR "CacheLib doesn't exist")
endif()
