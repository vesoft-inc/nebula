# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
  graph_stats_obj
  SHARED
  GraphStats.cpp
  )
  target_link_libraries(graph_stats_obj PUBLIC storage_client_stats_obj stats_obj )
