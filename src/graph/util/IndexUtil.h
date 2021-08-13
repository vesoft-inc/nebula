/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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

  static StatusOr<DataSet> toDescIndex(const meta::cpp2::IndexItem &indexItem);

  static StatusOr<DataSet> toShowCreateIndex(bool isTagIndex,
                                             const std::string &indexName,
                                             const meta::cpp2::IndexItem &indexItem);

  static Expression::Kind reverseRelationalExprKind(Expression::Kind kind);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_INDEXUTIL_H_
