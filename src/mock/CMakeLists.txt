# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

nebula_add_library(
    mock_obj OBJECT
    MetaCache.cpp
    StorageCache.cpp
    MockMetaServiceHandler.cpp
    MockStorageServiceHandler.cpp
    test/TestMain.cpp
    test/TestEnv.cpp
    test/TestBase.cpp
)

# nebula_add_subdirectory(test)
