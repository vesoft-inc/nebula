add_subdirectory(datamanlite)

add_library(nebula_native_client
            SHARED
            $<TARGET_OBJECTS:nebula_codec_obj>
            com_vesoft_client_NativeClient.cpp)


target_link_libraries(nebula_native_client
                      -lglog
                      -lgflags
                      -static-libgcc
                      -static-libstdc++
                      -lpthread
                      -ldl)

