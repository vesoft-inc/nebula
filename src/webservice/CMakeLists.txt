nebula_add_library(
    ws_obj OBJECT
    WebService.cpp
    Router.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
    StatusHandler.cpp
    SysInfoHandler.cpp
)

nebula_add_library(
    ws_common_obj OBJECT
    Common.cpp
)

nebula_add_subdirectory(test)
