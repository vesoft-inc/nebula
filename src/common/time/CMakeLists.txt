nebula_add_library(
    time_obj OBJECT
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)

add_subdirectory(test)
