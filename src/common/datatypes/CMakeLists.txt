# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_library(
    datatypes_obj OBJECT
    Date.cpp
    Path.cpp
    Value.cpp
    HostAddr.cpp
    Edge.cpp
    Vertex.cpp
    Map.cpp
    List.cpp
    Set.cpp
)

nebula_add_subdirectory(test)
