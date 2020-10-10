add_subdirectory(datamanlite)

add_library(nebula_codec
            SHARED
            $<TARGET_OBJECTS:nebula_codec_obj>
            com_vesoft_nebula_NebulaCodec.cpp)

set(LINUX_DEPEND_LIB -static-libgcc)
if (APPLE)
    set(LINUX_DEPEND_LIB)
endif()


target_link_libraries(nebula_codec
                      -lglog
                      -lgflags
                      ${LINUX_DEPEND_LIB}
                      -static-libstdc++
                      -lpthread
                      -ldl)

