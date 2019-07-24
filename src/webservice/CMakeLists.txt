nebula_add_library(
    ws_obj OBJECT
    WebService.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
)

add_subdirectory(test)
