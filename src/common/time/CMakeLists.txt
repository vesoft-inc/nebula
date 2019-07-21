add_library(
    time_obj OBJECT
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)
nebula_add_base_obj(time_obj)

add_subdirectory(test)

