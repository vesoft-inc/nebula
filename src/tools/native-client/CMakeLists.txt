include_directories($ENV{JAVA_HOME}/include
                    $ENV{JAVA_HOME}/include/linux)

add_compile_options(-std=c++14 -static-libstdc++ -fPIC)

add_library(nebula_native_client SHARED 
            $<TARGET_OBJECTS:dataman_obj>
            src/main/cpp/com_vesoft_client_NativeClient.cpp)

target_link_libraries(nebula_native_client dataman)
