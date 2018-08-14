add_library(time_obj OBJECT Duration.cpp)

add_dependencies(time_obj base_obj)

add_subdirectory(test)

