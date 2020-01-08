nebula_add_library(
    time_obj OBJECT
    detail/TscHelper.cpp
    Duration.cpp
    WallClock.cpp
)

nebula_add_library(
    tsc_calibrator_obj OBJECT
    detail/TscCalibrator.cpp
)

nebula_add_subdirectory(test)
