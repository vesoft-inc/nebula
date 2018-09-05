add_library(
        concurrent_obj OBJECT
        Barrier.cpp
        Latch.cpp
)
add_subdirectory(test)
