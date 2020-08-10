# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    base_obj OBJECT
    Base.cpp
    Cord.cpp
    Status.cpp
    SanitizerOptions.cpp
    SignalHandler.cpp
    SlowOpTracker.cpp
    StringValue.cpp
)

nebula_add_subdirectory(test)
