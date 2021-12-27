# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

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
    Geography.cpp
    Duration.cpp
)

nebula_add_subdirectory(test)
