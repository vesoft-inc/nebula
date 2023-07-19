# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    geo_index_obj OBJECT
    GeoIndex.cpp
)

nebula_add_subdirectory(io)
nebula_add_subdirectory(test)
