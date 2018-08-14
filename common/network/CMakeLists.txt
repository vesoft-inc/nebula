add_library(network_obj OBJECT NetworkUtils.cpp ThriftSocketManager.cpp)

add_dependencies(network_obj base_obj)

add_subdirectory(test)

