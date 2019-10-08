nebula_add_subdirectory(storage-perf)
nebula_add_subdirectory(simple-kv-verify)

if (ENABLE_NATIVE)
    add_subdirectory(native-client)
endif()

if (NOT SKIP_JAVA_CLIENT)
    add_subdirectory(importer)
else()
    message(STATUS "Skip building the importer")
endif()
