add_library(process_obj OBJECT ProcessUtils.cpp)

add_dependencies(process_obj base_obj)

add_subdirectory(test)
