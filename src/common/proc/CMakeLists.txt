add_library(proc_obj OBJECT ProcAccessor.cpp)

add_dependencies(proc_obj base_obj)

add_subdirectory(test)

