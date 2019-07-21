add_library(
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
nebula_add_base_obj(storage_service_handler)

add_library(
    storage_http_handler OBJECT
    StorageHttpStatusHandler.cpp
    StorageHttpDownloadHandler.cpp
)
nebula_add_base_obj(storage_http_handler)

add_library(
    storage_client OBJECT
    client/StorageClient.cpp
)
nebula_add_base_obj(storage_client)

add_subdirectory(test)
