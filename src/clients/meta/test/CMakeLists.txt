# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
    NAME file_based_cluster_id_man_test
    SOURCES FileBasedClusterIdManTest.cpp
    LIBRARIES
        file_based_cluster_id_man_obj
        base_obj
        fs_obj
        ssl_obj
    LIBRARIES gtest
)
