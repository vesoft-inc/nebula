add_subdirectory(datamanlite)

add_library(nebula_codec
            SHARED
            $<TARGET_OBJECTS:nebula_codec_obj>
            com_vesoft_nebula_NebulaCodec.cpp)


target_link_libraries(nebula_codec
                      -lglog
                      -lgflags
                      -static-libgcc
                      -static-libstdc++
                      -lpthread
                      -ldl)

