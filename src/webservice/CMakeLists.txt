add_library(
    ws_obj OBJECT
    WebService.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
)
add_dependencies(
    ws_obj
    base_obj
    thread_obj
)

add_subdirectory(test)

