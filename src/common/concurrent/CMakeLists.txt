add_library(
        concurrent_obj OBJECT
        Barrier.cpp
        Latch.cpp
)
nebula_add_base_obj(concurrent_obj)

add_subdirectory(test)
