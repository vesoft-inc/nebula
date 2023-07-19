// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

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

  // Rewrites the list comprehension expression so that the innerVar_ will be replaced by a newly
  // generated variable.
  // This is to prevent duplicate variable names existing both inside and outside the expression.
  static void rewriteLC(QueryContext *qctx,
                        ListComprehensionExpression *lc,
                        const std::string &oldVarName);

  // Rewrites the predicate comprehension expression so that the innerVar_ will be replaced by a
  // newly generated variable. This is to prevent duplicate variable names existing both inside and
  // outside the expression.
  static void rewritePred(QueryContext *qctx,
                          PredicateExpression *pred,
                          const std::string &oldVarName);

  // Rewrites the list reduce expression so that the innerVar_ and the accumulator_ will be replaced
  // by newly generated variables. This is to prevent duplicate variable names existing both inside
  // and outside the expression.
  static void rewriteReduce(QueryContext *qctx,
                            ReduceExpression *reduce,
                            const std::string &oldAccName,
                            const std::string &oldVarName);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_PARSERUTIL_H_
