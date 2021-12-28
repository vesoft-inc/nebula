# note: standalone version don't have dependent test tools for now.

if(NOT ENABLE_STANDALONE_VERSION)
nebula_add_subdirectory(storage-perf)
nebula_add_subdirectory(simple-kv-verify)
endif()
nebula_add_subdirectory(meta-dump)
nebula_add_subdirectory(db-dump)
nebula_add_subdirectory(db-upgrade)
