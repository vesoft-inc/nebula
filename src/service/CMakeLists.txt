
nebula_add_library(
    graph_flags_obj OBJECT
    GraphFlags.cpp
)

nebula_add_library(
    service_obj OBJECT
    GraphService.cpp
)

nebula_add_library(
    query_engine_obj OBJECT
    ExecutionContext.cpp
    QueryEngine.cpp
    QueryInstance.cpp
)

nebula_add_library(
    session_obj OBJECT
    ClientSession.cpp
    SessionManager.cpp
)

nebula_add_library(
    graph_http_obj OBJECT
    GraphHttpHandler.cpp
)
