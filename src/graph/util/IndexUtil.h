/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_UTIL_INDEXUTIL_H_
#define GRAPH_UTIL_INDEXUTIL_H_

#include "common/base/StatusOr.h"
#include "graph/util/SchemaUtil.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class IndexUtil final {
 public:
  IndexUtil() = delete;

  static Status validateColumns(const std::vector<std::string> &fields);

  static Status validateIndexParams(const std::vector<IndexParamItem *> &params,
                                    meta::cpp2::IndexParams &indexParams);

  static StatusOr<DataSet> toDescIndex(const meta::cpp2::IndexItem &indexItem);

  static StatusOr<DataSet> toShowCreateIndex(bool isTagIndex,
                                             const std::string &indexName,
                                             const meta::cpp2::IndexItem &indexItem);

  static Expression::Kind reverseRelationalExprKind(Expression::Kind kind);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_INDEXUTIL_H_
