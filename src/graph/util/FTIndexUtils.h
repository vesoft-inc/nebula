// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_UTIL_FT_INDEXUTIL_H_
#define GRAPH_UTIL_FT_INDEXUTIL_H_

#include "clients/meta/MetaClient.h"
#include "common/base/StatusOr.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"
#include "graph/util/SchemaUtil.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class FTIndexUtils final {
 public:
  FTIndexUtils() = delete;
  // Checks if the filter expression is full-text search related
  static bool needTextSearch(const Expression* expr);
  // Checks meta client and returns the full-text search client if there is one
  static StatusOr<std::vector<nebula::plugin::HttpClient>> getTSClients(meta::MetaClient* client);
  // Checks if the full-text index exists
  static StatusOr<bool> checkTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                                     const std::string& index);
  // Drops the full-text index
  static StatusOr<bool> dropTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                                    const std::string& index);

  // Clears the full-text index data, but keeps the index schema
  static StatusOr<bool> clearTSIndex(const std::vector<nebula::plugin::HttpClient>& tsClients,
                                     const std::string& index);

  // Converts TextSearchExpression into a relational expresion that could be pushed down
  static StatusOr<Expression*> rewriteTSFilter(
      ObjectPool* pool,
      bool isEdge,
      Expression* expr,
      const std::string& index,
      const std::vector<nebula::plugin::HttpClient>& tsClients);

  // Performs full-text search using elastic search adapter
  // Search type is defiend by the expression kind
  static StatusOr<std::vector<std::string>> textSearch(
      Expression* expr,
      const std::string& index,
      const std::vector<nebula::plugin::HttpClient>& tsClients);

  // Picks a random full-text search client from the given list
  static const nebula::plugin::HttpClient& randomFTClient(
      const std::vector<nebula::plugin::HttpClient>& tsClients);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_FT_INDEXUTIL_H_
