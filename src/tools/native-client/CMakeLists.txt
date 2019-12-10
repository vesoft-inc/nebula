# locate jni header
include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)

nebula_add_library(nebula_utils SHARED
            $<TARGET_OBJECTS:dataman_obj>
            $<TARGET_OBJECTS:fs_obj>
            $<TARGET_OBJECTS:base_obj>
            $<TARGET_OBJECTS:schema_obj>
            $<TARGET_OBJECTS:network_obj>
            $<TARGET_OBJECTS:storage_thrift_obj>
            $<TARGET_OBJECTS:common_thrift_obj>
            $<TARGET_OBJECTS:thrift_obj>
            $<TARGET_OBJECTS:meta_client>
            $<TARGET_OBJECTS:stats_obj>
            $<TARGET_OBJECTS:time_obj>
            $<TARGET_OBJECTS:meta_thrift_obj>
            src/main/cpp/com_vesoft_nebula_NebulaUtils.cpp)


target_link_libraries(nebula_utils
                      ${OPENSSL_SSL_LIBRARY}
                      ${OPENSSL_CRYPTO_LIBRARY}
                      ${THRIFT_LIBRARIES}
                      -lfolly
                      -lboost_context
                      -lglog
                      -lgflags
                      -lsnappy
                      -lz
                      -lzstd
                      -levent
                      -ldouble-conversion
                      -static-libgcc
                      -static-libstdc++
                      -lpthread
                      -ldl)

add_custom_target(
    nebula_java_utils ALL
    COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${CMAKE_CURRENT_SOURCE_DIR} ">" "./_build.log" "2>&1"
    DEPENDS nebula_utils
)
