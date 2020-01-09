nebula_add_library(
    ws_common_obj OBJECT
    Common.cpp
)

nebula_add_library(
    ws_obj OBJECT
    WebService.cpp
    Router.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
    StatusHandler.cpp
)

add_dependencies(ws_obj ws_common_obj)

nebula_add_subdirectory(test)
