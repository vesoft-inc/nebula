nebula_add_library(
    storage_service_handler OBJECT
    StorageServiceHandler.cpp
    QueryBaseProcessor.cpp
    AddVerticesProcessor.cpp
    AddEdgesProcessor.cpp
    QueryBoundProcessor.cpp
    QueryVertexPropsProcessor.cpp
    QueryEdgePropsProcessor.cpp
    QueryStatsProcessor.cpp
)

nebula_add_library(
    storage_http_handler OBJECT
    StorageHttpIngestHandler.cpp
    StorageHttpStatusHandler.cpp
    StorageHttpDownloadHandler.cpp
)

nebula_add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)

add_subdirectory(test)
