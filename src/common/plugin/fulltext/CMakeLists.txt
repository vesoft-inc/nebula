# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    es_adapter_obj OBJECT
    elasticsearch/ESAdapter.cpp
    elasticsearch/ESClient.cpp
)

nebula_add_subdirectory(test)
