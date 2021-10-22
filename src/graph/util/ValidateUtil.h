/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_UTIL_VALIDATE_UTIL_H_
#define GRAPH_UTIL_VALIDATE_UTIL_H_
#include "common/base/StatusOr.h"
#include "common/expression/Expression.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {
class QueryContext;
class PlanNode;
struct Over;

class ValidateUtil final {
 public:
  ValidateUtil() = delete;

  static Status validateStep(const StepClause* clause, StepClause& step);

  static Status validateOver(QueryContext* qctx, const OverClause* clause, Over& over);

  static Status invalidLabelIdentifiers(const Expression* expr);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_VALIDATE_UTIL_H_
