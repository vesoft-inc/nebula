add_library(
        concurrent_obj OBJECT
        Barrier.cpp
        Latch.cpp
)
add_dependencies(concurrent_obj base_obj)

add_subdirectory(test)
