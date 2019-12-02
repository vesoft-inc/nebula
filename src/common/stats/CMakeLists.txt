nebula_add_library(
    stats_obj
    OBJECT
    StatsManager.cpp
    MetricsSerializer.cpp
    Stats.cpp
)

nebula_add_subdirectory(test)


