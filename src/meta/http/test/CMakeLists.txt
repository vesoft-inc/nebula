# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
  NAME meta_http_download_test
  SOURCES MetaHttpDownloadHandlerTest.cpp
  )

target_link_libraries(
        meta_http_download_test
        PRIVATE
    storage_http_handler
    meta_http_handler
    ${meta_test_deps}
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    wangle
    gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
nebula_add_test(
  NAME meta_http_ingest_test
  SOURCES MetaHttpIngestHandlerTest.cpp
  )

target_link_libraries(
        meta_http_ingest_test
        PRIVATE
    storage_http_handler
    meta_http_handler
    ${meta_test_deps}
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    wangle
    gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)

nebula_add_test(
   NAME
       meta_http_replace_test
   SOURCES
       MetaHttpReplaceHandlerTest.cpp
)
target_link_libraries(
        meta_http_replace_test
        PRIVATE
       storage_http_handler
       meta_http_handler
       ${meta_test_deps}
       ${ROCKSDB_LIBRARIES}
       ${THRIFT_LIBRARIES}
       ${PROXYGEN_LIBRARIES}
       wangle
       gtest
        ${THRIFT_LIBRARIES}
        ${NEBULA_LIBS}
)
