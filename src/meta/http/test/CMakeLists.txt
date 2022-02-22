# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_test(
  NAME meta_http_download_test
  SOURCES MetaHttpDownloadHandlerTest.cpp
  LIBRARIES
    storage_http_handler
    meta_http_handler
    ${meta_test_deps}
  LIBRARIES
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    wangle
    gtest
  )

nebula_add_test(
  NAME meta_http_ingest_test
  SOURCES MetaHttpIngestHandlerTest.cpp
  LIBRARIES
    storage_http_handler
    meta_http_handler
    ${meta_test_deps}
  LIBRARIES
    ${ROCKSDB_LIBRARIES}
    ${THRIFT_LIBRARIES}
    ${PROXYGEN_LIBRARIES}
    wangle
    gtest
  )


nebula_add_test(
   NAME
       meta_http_replace_test
   SOURCES
       MetaHttpReplaceHandlerTest.cpp
   LIBRARIES
       storage_http_handler
       meta_http_handler
       ${meta_test_deps}
   LIBRARIES
       ${ROCKSDB_LIBRARIES}
       ${THRIFT_LIBRARIES}
       ${PROXYGEN_LIBRARIES}
       wangle
       gtest
)
