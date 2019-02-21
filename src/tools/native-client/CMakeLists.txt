include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)

add_library(nebula_native_client SHARED 
            $<TARGET_OBJECTS:dataman_obj>
            $<TARGET_OBJECTS:fs_obj>
            $<TARGET_OBJECTS:base_obj>
            $<TARGET_OBJECTS:meta_obj>
            $<TARGET_OBJECTS:network_obj>
            $<TARGET_OBJECTS:storage_thrift_obj>
            src/main/cpp/com_vesoft_client_NativeClient.cpp)


target_link_libraries(nebula_native_client
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
                      -lstdc++
                      -lpthread
                      -ldl)
