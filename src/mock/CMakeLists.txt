
nebula_add_library(
    mock_obj SHARED
    MockCluster.cpp
    MockData.cpp
    AdHocIndexManager.cpp
    AdHocSchemaManager.cpp
)
target_link_libraries(mock_obj PUBLIC meta_manager_obj  expression_obj meta_obj graph_storage_service_handler  meta_service_handler kvstore_obj storage_admin_service_handler )

