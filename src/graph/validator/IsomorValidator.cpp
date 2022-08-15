/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "graph/validator/IsomorValidator.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/ValidateUtil.h"
#include "graph/visitor/DeducePropsVisitor.h"
namespace nebula {
namespace graph {
Status IsomorValidator::validateImpl() {
  auto *fSentence = static_cast<IsomorSentence *>(sentence_);
  fetchCtx_ = getContext<IsomorContext>();
  NG_RETURN_IF_ERROR(validateGraph(fSentence->graphs_()), fetchCtx_);
  return Status::OK();
}
// Check validity of tags specified in sentence
Status IsomorValidator::validateGraph(const NameLabelList *nameLabels,
                                      std::unique_ptr<IsomorContext> &fetchCtx) {
  auto graphs = nameLabels->labels();

  // the first graph is data graph, and the second graph is query graph
  fetchCtx->querySpace = qctx_->schemaMng()->toGraphSpaceID(*graphs[0]);
  fetchCtx->dataSpace = qctx_->schemaMng()->toGraphSpaceID(*graphs[1]);
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
