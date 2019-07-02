add_library(
    time_obj OBJECT
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)
add_dependencies(time_obj base_obj)

add_subdirectory(test)

