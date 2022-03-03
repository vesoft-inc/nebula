# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


nebula_add_library(
  meta_http_handler SHARED
  MetaHttpIngestHandler.cpp
  MetaHttpDownloadHandler.cpp
  MetaHttpReplaceHostHandler.cpp
  )

target_link_libraries(meta_http_handler PUBLIC   "${Glog_LIBRARY}")
nebula_add_subdirectory(test)
