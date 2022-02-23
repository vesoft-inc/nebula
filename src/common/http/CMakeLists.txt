# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(http_client_obj SHARED HttpClient.cpp)
target_link_libraries(http_client_obj PRIVATE process_obj)

nebula_add_subdirectory(test)
