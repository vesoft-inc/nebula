# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    graph_session_obj SHARED
    GraphSessionManager.cpp
    ClientSession.cpp
)

target_link_libraries(graph_session_obj PUBLIC thread_obj graph_stats_obj)

