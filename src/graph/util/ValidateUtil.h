// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

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

  // Checks the upper bound steps of clause is greater than its lower bound.
  // TODO(Aiee) Seems this method not only checks the clause but also set the clause into step which
  // is in the context. Consider separating it.
  static Status validateStep(const StepClause* clause, StepClause& step);

  // Retrieves edges from schema manager and validate the over clause.
  static Status validateOver(QueryContext* qctx, const OverClause* clause, Over& over);

  // Returns a status error if the expr contains any labelExpression.
  static Status invalidLabelIdentifiers(const Expression* expr);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_VALIDATE_UTIL_H_
