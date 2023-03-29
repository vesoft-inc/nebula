/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_GOVALIDATOR_H_
#define GRAPH_VALIDATOR_GOVALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {
class GoValidator final : public Validator {
 public:
  using VertexProp = nebula::storage::cpp2::VertexProp;
  using EdgeProp = nebula::storage::cpp2::EdgeProp;

  GoValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  AstContext* getAstContext() override {
    return goCtx_.get();
  }

  Status validateWhere(WhereClause* where);

  Status validateYield(YieldClause* yield);

  Status validateTruncate(TruncateClause* truncate);

  Status buildColumns();

  Status extractTagIds();

  Status extractPropExprs(const Expression* expr, std::unordered_set<std::string>& uniqueCols);

  Expression* rewrite2VarProp(const Expression* expr);

  Expression* rewriteVertexEdge2EdgeProp(const Expression* expr);

  bool checkDstPropOrVertexExist(const Expression* expr);

 private:
  std::unique_ptr<GoContext> goCtx_;

  YieldColumns* inputPropCols_{nullptr};
  std::unordered_map<std::string, YieldColumn*> propExprColMap_;
  std::vector<TagID> tagIds_;
};
}  // namespace graph
}  // namespace nebula
#endif
