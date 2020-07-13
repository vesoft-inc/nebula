# locate jni header
message(STATUS "JAVA_HOME is " $ENV{JAVA_HOME})
include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)

execute_process(
    COMMAND ldd --version
    COMMAND head -1
    COMMAND cut -d ")" -f 2
    COMMAND cut -d " " -f 2
    OUTPUT_VARIABLE GLIBC_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "Glibc version for native client is " ${GLIBC_VERSION})

if (GLIBC_VERSION VERSION_LESS "2.17")
    set(GETTIME_LIB rt)
else()
    set(GETTIME_LIB)
endif()

nebula_add_library(nebula_native_client SHARED
            $<TARGET_OBJECTS:dataman_obj>
            $<TARGET_OBJECTS:fs_obj>
            $<TARGET_OBJECTS:base_obj>
            $<TARGET_OBJECTS:schema_obj>
            $<TARGET_OBJECTS:network_obj>
            $<TARGET_OBJECTS:storage_thrift_obj>
            $<TARGET_OBJECTS:common_thrift_obj>
            $<TARGET_OBJECTS:thrift_obj>
            $<TARGET_OBJECTS:meta_client>
            $<TARGET_OBJECTS:gflags_man_obj>
            $<TARGET_OBJECTS:stats_obj>
            $<TARGET_OBJECTS:time_obj>
            $<TARGET_OBJECTS:meta_thrift_obj>
            $<TARGET_OBJECTS:thread_obj>
            src/main/cpp/com_vesoft_client_NativeClient.cpp)


target_link_libraries(nebula_native_client
                      -Wl,--no-undefined
                      ${THRIFT_LIBRARIES}
                      -lwangle
                      -lfolly
                      -lboost_context
                      -lboost_system
                      -lglog
                      -lgflags
                      -lsnappy
                      -lz
                      -lzstd
                      -lbz2
                      -llzma
                      -llz4
                      -levent
                      -ldouble-conversion
                      ${OPENSSL_SSL_LIBRARY}
                      ${OPENSSL_CRYPTO_LIBRARY}
                      -static-libgcc
                      -static-libstdc++
                      -lpthread
                      -ldl
                      ${GETTIME_LIB})


add_custom_target(
    nebula_native_java_client ALL
    COMMAND "NEBULA_BUILD_ROOT=${CMAKE_BINARY_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${CMAKE_CURRENT_SOURCE_DIR} ">" "./_build.log" "2>&1"
    DEPENDS nebula_native_client
)
