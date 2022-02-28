# The base_obj is a basic library. Other libraries are not allowed to depend on it.

nebula_add_library(
    keyutils_obj SHARED
    NebulaKeyUtils.cpp
    IndexKeyUtils.cpp
    OperationKeyUtils.cpp
)
target_link_libraries(keyutils_obj PUBLIC "${Glog_LIBRARY}" time_utils_obj   )

nebula_add_library(
    meta_keyutils_obj SHARED
    MetaKeyUtils.cpp
)

nebula_add_library(
    log_str_list_iterator_obj SHARED
    LogStrListIterator.cpp
)


nebula_add_subdirectory(test)
