/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_UTIL_PARSERUTIL_H_
#define GRAPH_UTIL_PARSERUTIL_H_

#include "common/base/StatusOr.h"
#include "graph/context/QueryContext.h"
#include "graph/visitor/RewriteVisitor.h"
#include "parser/MaintainSentences.h"

namespace nebula {
namespace graph {

class ParserUtil final {
 public:
  ParserUtil() = delete;

  static bool isLabel(const Expression *expr);
  static void rewriteLC(QueryContext *qctx,
                        ListComprehensionExpression *lc,
                        const std::string &oldVarName);

  static void rewritePred(QueryContext *qctx,
                          PredicateExpression *pred,
                          const std::string &oldVarName);

  static void rewriteReduce(QueryContext *qctx,
                            ReduceExpression *reduce,
                            const std::string &oldAccName,
                            const std::string &oldVarName);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_PARSERUTIL_H_
