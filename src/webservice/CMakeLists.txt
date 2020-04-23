# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    ws_obj OBJECT
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
    ws_common_obj OBJECT
    Common.cpp
)

nebula_add_subdirectory(test)
