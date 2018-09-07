add_library(client_cpp_obj OBJECT GraphDbClient.cpp)
add_dependencies(
    client_cpp_obj
    base_obj
    vgraph_thrift_obj
)

#add_subdirectory(test)


