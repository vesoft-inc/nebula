nebula_add_library(
    service_obj OBJECT
    GraphService.cpp
    GraphHttpHandler.cpp
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
    graph_gflags_obj OBJECT
    GraphFlags.cpp
)
