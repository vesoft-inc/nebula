add_library(http_client_obj OBJECT HttpClient.cpp)

add_dependencies(http_client_obj process_obj base_obj)

nebula_add_subdirectory(test)
