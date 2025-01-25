/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_TEST_MOCKESADAPTER_H_
#define COMMON_PLUGIN_FULLTEXT_ELASTICSEARCH_TEST_MOCKESADAPTER_H_

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"

class MockESAdapter : public ESAdapter {
 public:
  MOCK_METHOD1(createIndex, StatusOr<bool>(const std::string&));
  MOCK_METHOD1(dropIndex, StatusOr<bool>(const std::string&));
  MOCK_METHOD1(indexExists, StatusOr<bool>(const std::string&));
  MOCK_METHOD2(query, StatusOr<ESQueryResult>(const std::string&, const std::string&));
  MOCK_METHOD5(vectorQuery,
               StatusOr<ESQueryResult>(const std::string&,
                                     const std::string&,
                                     double,
                                     int64_t,
                                     int64_t));
}; 