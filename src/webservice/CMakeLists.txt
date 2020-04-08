nebula_add_all_library(
    ws
    WebService.cpp
    Router.cpp
    NotFoundHandler.cpp
    GetFlagsHandler.cpp
    SetFlagsHandler.cpp
    GetStatsHandler.cpp
    StatusHandler.cpp
)

nebula_add_all_library(
    ws_common
    Common.cpp
)

nebula_add_subdirectory(test)
