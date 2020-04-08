nebula_add_all_library(
    time
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)

nebula_add_subdirectory(test)
