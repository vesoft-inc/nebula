/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/UnwindValidator.h"

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

Status UnwindValidator::validateImpl() {
  auto *unwindSentence = static_cast<UnwindSentence *>(sentence_);
  if (unwindSentence->alias().empty()) {
    return Status::SemanticError("Expression in UNWIND must be aliased (use AS)");
  }
  alias_ = unwindSentence->alias();
  unwindExpr_ = unwindSentence->expr()->clone();

  if (ExpressionUtils::findAny(unwindExpr_,
                               {Expression::Kind::kSrcProperty,
                                Expression::Kind::kDstProperty,
                                Expression::Kind::kVarProperty,
                                Expression::Kind::kLabel,
                                Expression::Kind::kAggregate,
                                Expression::Kind::kEdgeProperty})) {
    return Status::SemanticError("Not support `%s' in UNWIND sentence.",
                                 unwindExpr_->toString().c_str());
  }

  auto typeStatus = deduceExprType(unwindExpr_);
  NG_RETURN_IF_ERROR(typeStatus);
  outputs_.emplace_back(alias_, Value::Type::__EMPTY__);
  return Status::OK();
}

Status UnwindValidator::toPlan() {
  auto *unwind = Unwind::make(qctx_, nullptr, unwindExpr_, alias_);
  unwind->setColNames({alias_});
  unwind->setFromPipe(true);
  root_ = tail_ = unwind;
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
