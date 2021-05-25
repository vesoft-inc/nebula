# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

if (ENABLE_GDB_SCRIPT_SECTION)
    set(gdb_debug_script GdbDebugScript.cpp)
endif()

nebula_add_library(
    base_obj OBJECT
    Base.cpp
    Cord.cpp
    Status.cpp
    SanitizerOptions.cpp
    SignalHandler.cpp
    SlowOpTracker.cpp
    StringValue.cpp
    Memory.cpp
    ${gdb_debug_script}
)

nebula_add_subdirectory(test)
