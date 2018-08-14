add_library(
    consensus_obj OBJECT
    FileBasedWal.cpp
)
add_dependencies(consensus_obj common)

add_subdirectory(test)

