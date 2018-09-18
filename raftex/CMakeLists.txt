add_library(
    raftex_obj OBJECT
    FileBasedWal.cpp
)
add_dependencies(raftex_obj common)

add_subdirectory(test)

