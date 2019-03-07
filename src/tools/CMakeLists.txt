add_subdirectory(storage-perf)
if (ENABLE_NATIVE)
    # need to find jni.h
    find_package(JNI REQUIRED)
    list(APPEND CMAKE_INCLUDE_PATH ${JAVA_INCLUDE_PATH})
    add_subdirectory(native-client)
endif()

if (NOT SKIP_JAVA_CLIENT)
    add_subdirectory(importer)
else()
    message(STATUS "Skip building the importer")
endif()