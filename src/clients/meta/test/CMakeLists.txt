# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

nebula_add_test(
    NAME file_based_cluster_id_man_test
    SOURCES FileBasedClusterIdManTest.cpp
    OBJECTS
        $<TARGET_OBJECTS:file_based_cluster_id_man_obj>
        $<TARGET_OBJECTS:base_obj>
        $<TARGET_OBJECTS:fs_obj>
    LIBRARIES gtest
)
