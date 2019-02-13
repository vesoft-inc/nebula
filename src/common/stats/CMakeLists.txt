add_library(stats_obj OBJECT StatsManager.cpp)

add_dependencies(stats_obj base_obj)

add_subdirectory(test)


