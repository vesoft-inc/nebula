# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
#
# The base_obj is a basic library. Other libraries are not allowed to depend on it.

nebula_add_library(
    keyutils_obj OBJECT
    NebulaKeyUtils.cpp
    IndexKeyUtils.cpp
    OperationKeyUtils.cpp
)

nebula_add_library(
    meta_keyutils_obj OBJECT
    MetaKeyUtils.cpp
)

nebula_add_library(
    log_str_list_iterator_obj OBJECT
    LogStrListIterator.cpp
)


nebula_add_subdirectory(test)
