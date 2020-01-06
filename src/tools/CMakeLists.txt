nebula_add_subdirectory(storage-perf)
nebula_add_subdirectory(simple-kv-verify)
nebula_add_subdirectory(dump-edges)
nebula_add_subdirectory(db-dump)

if (ENABLE_NATIVE)
    add_subdirectory(native-client)
endif()
