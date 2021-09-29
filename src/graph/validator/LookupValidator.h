/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef _VALIDATOR_LOOKUP_VALIDATOR_H_
#define _VALIDATOR_LOOKUP_VALIDATOR_H_

#include "common/base/Base.h"
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"

namespace nebula {

namespace meta {
class NebulaSchemaProvider;
}  // namespace meta

namespace graph {

struct LookupContext;

class LookupValidator final : public Validator {
 public:
  LookupValidator(Sentence* sentence, QueryContext* context);

  AstContext* getAstContext() override;

 private:
  Status validateImpl() override;

  Status validateFrom();
  Status validateYield();
  Status validateFilter();
  Status validateYieldTag();
  Status validateYieldEdge();
  Status validateLimit();

  StatusOr<Expression*> checkFilter(Expression* expr);
  StatusOr<Expression*> checkRelExpr(RelationalExpression* expr);
  StatusOr<std::string> checkTSExpr(Expression* expr);
  StatusOr<Value> checkConstExpr(Expression* expr,
                                 const std::string& prop,
                                 const Expression::Kind kind);

  StatusOr<Expression*> rewriteRelExpr(RelationalExpression* expr);
  Expression* reverseRelKind(RelationalExpression* expr);

  const LookupSentence* sentence() const;
  int32_t schemaId() const;
  GraphSpaceID spaceId() const;

 private:
  Status getSchemaProvider(std::shared_ptr<const meta::NebulaSchemaProvider>* provider) const;
  StatusOr<Expression*> genTsFilter(Expression* filter);
  StatusOr<Expression*> handleLogicalExprOperands(LogicalExpression* lExpr);
  Status extractSchemaProp();
  void extractExprProps();

  std::unique_ptr<LookupContext> lookupCtx_;
  std::vector<nebula::plugin::HttpClient> tsClients_;
  ExpressionProps exprProps_;
  std::vector<std::string> idxReturnCols_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_LOOKUP_VALIDATOR_H_
