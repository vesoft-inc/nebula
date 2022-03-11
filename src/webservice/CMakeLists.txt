# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    ws_obj SHARED
    WebService.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
    Router.cpp
    StatusHandler.cpp
)

set_target_properties(ws_obj PROPERTIES COMPILE_FLAGS "-Wno-error=format-security")

nebula_add_library(
    ws_common_obj SHARED
    Common.cpp
)
target_link_libraries(ws_obj PUBLIC "${Glog_LIBRARY}" ws_common_obj  )

nebula_add_subdirectory(test)
