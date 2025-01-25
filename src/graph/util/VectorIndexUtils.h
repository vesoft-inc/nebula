/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_UTIL_VECTORINDEXUTILS_H_
#define GRAPH_UTIL_VECTORINDEXUTILS_H_

#include "clients/meta/MetaClient.h"
#include "common/base/StatusOr.h"
#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "graph/util/SchemaUtil.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class VectorIndexUtils final {
 public:
  VectorIndexUtils() = delete;

  // Checks if the expression is vector search related
  static bool needVectorSearch(const Expression* expr);

  static StatusOr<::nebula::plugin::ESAdapter> getESAdapter(meta::MetaClient* client);

  // Converts VectorSearchExpression into a relational expression that could be pushed down
  static StatusOr<Expression*> rewriteVectorFilter(ObjectPool* pool,
                                                 bool isEdge,
                                                 Expression* expr,
                                                 const std::string& index,
                                                 ::nebula::plugin::ESAdapter& esAdapter);

  // Performs vector similarity search using elastic search adapter
  static StatusOr<nebula::plugin::ESQueryResult> vectorSearch(Expression* expr,
                                                            const std::string& index,
                                                            ::nebula::plugin::ESAdapter& esAdapter);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_UTIL_VECTORINDEXUTILS_H_ 