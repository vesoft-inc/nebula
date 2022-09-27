# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    raftex_obj OBJECT
    RaftLogIterator.cpp
    RaftPart.cpp
    RaftexService.cpp
    Host.cpp
    SnapshotManager.cpp
    ../LogEncoder.cpp
)

nebula_add_subdirectory(test)
