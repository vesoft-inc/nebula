nebula_add_subdirectory(storage-perf)
nebula_add_subdirectory(simple-kv-verify)
nebula_add_subdirectory(dump-edges)

if (ENABLE_NATIVE)
    add_subdirectory(native-client)
endif()
