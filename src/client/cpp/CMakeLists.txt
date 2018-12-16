add_library(client_cpp_obj OBJECT GraphClient.cpp)
add_dependencies(
    client_cpp_obj
    base_obj
    graph_thrift_obj
)

#add_subdirectory(test)


