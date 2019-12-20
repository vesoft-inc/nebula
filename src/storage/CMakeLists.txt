nebula_add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    StorageFlags.cpp
    query/QueryBaseProcessor.cpp
    query/QueryBoundProcessor.cpp
    query/QueryVertexPropsProcessor.cpp
    query/QueryEdgePropsProcessor.cpp
    query/QueryStatsProcessor.cpp
    query/QueryEdgeKeysProcessor.cpp
    mutate/AddVerticesProcessor.cpp
    mutate/AddEdgesProcessor.cpp
    mutate/DeleteEdgesProcessor.cpp
    mutate/DeleteVertexProcessor.cpp
    mutate/UpdateVertexProcessor.cpp
    mutate/UpdateEdgeProcessor.cpp
    kv/PutProcessor.cpp
    kv/GetProcessor.cpp
    admin/CreateCheckpointProcessor.cpp
    admin/DropCheckpointProcessor.cpp
    admin/SendBlockSignProcessor.cpp
)

nebula_add_library(
    storage_http_handler OBJECT
    http/StorageHttpIngestHandler.cpp
    http/StorageHttpStatusHandler.cpp
    http/StorageHttpDownloadHandler.cpp
    http/StorageHttpAdminHandler.cpp
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
