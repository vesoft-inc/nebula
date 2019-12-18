nebula_add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    StorageFlags.cpp
    QueryBaseProcessor.cpp
    AddVerticesProcessor.cpp
    AddEdgesProcessor.cpp
    DeleteEdgesProcessor.cpp
    DeleteVertexProcessor.cpp
    QueryBoundProcessor.cpp
    QueryVertexPropsProcessor.cpp
    QueryEdgePropsProcessor.cpp
    QueryStatsProcessor.cpp
    QueryEdgeKeysProcessor.cpp
    UpdateVertexProcessor.cpp
    UpdateEdgeProcessor.cpp
    PutProcessor.cpp
    GetProcessor.cpp
    CreateCheckpointProcessor.cpp
    DropCheckpointProcessor.cpp
    SendBlockSignProcessor.cpp
)

nebula_add_library(
    storage_http_handler OBJECT
    StorageHttpIngestHandler.cpp
    StorageHttpStatusHandler.cpp
    StorageHttpDownloadHandler.cpp
    StorageHttpAdminHandler.cpp
)

nebula_add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)

nebula_add_library(
    storage_server OBJECT
    StorageServer.cpp
)

nebula_add_subdirectory(test)
