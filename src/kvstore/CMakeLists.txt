# Copyright (c) 2022 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    kvstore_obj OBJECT
    Part.cpp
    RocksEngine.cpp
    PartManager.cpp
    NebulaStore.cpp
    RocksEngineConfig.cpp
    NebulaSnapshotManager.cpp
    RateLimiter.cpp
)

target_link_libraries(kvstore_obj
    PUBLIC
        base_obj
        fs_obj
        embedding_client_obj
)

nebula_add_library(
    disk_man_obj OBJECT
    DiskManager.cpp
)

nebula_add_subdirectory(raftex)
nebula_add_subdirectory(listener)
nebula_add_subdirectory(wal)
nebula_add_subdirectory(stats)
nebula_add_subdirectory(test)

#nebula_add_subdirectory(plugins)

