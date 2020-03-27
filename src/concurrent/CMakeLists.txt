nebula_add_library(
    concurrent_obj OBJECT
    Barrier.cpp
    Latch.cpp
)

nebula_add_subdirectory(test)
