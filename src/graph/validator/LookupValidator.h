/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef _VALIDATOR_LOOKUP_VALIDATOR_H_
#define _VALIDATOR_LOOKUP_VALIDATOR_H_

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
  Status validateWhere();
  Status validateYieldTag();
  Status validateYieldEdge();
  Status validateYieldColumn(YieldColumn* col, bool isEdge);

  StatusOr<Expression*> checkFilter(Expression* expr);
  Status checkRelExpr(RelationalExpression* expr);
  Status checkGeoPredicate(const Expression* expr) const;
  StatusOr<std::string> checkTSExpr(Expression* expr);
  StatusOr<Expression*> checkConstExpr(Expression* expr,
                                       const std::string& prop,
                                       const Expression::Kind kind);
  StatusOr<Expression*> rewriteRelExpr(RelationalExpression* expr);
  StatusOr<Expression*> rewriteGeoPredicate(Expression* expr);
  Expression* reverseRelKind(RelationalExpression* expr);
  Expression* reverseGeoPredicate(Expression* expr);

  const LookupSentence* sentence() const;
  int32_t schemaId() const;
  GraphSpaceID spaceId() const;

 private:
  Status getSchemaProvider(std::shared_ptr<const meta::NebulaSchemaProvider>* provider) const;
  StatusOr<Expression*> genTsFilter(Expression* filter);
  StatusOr<Expression*> handleLogicalExprOperands(LogicalExpression* lExpr);
  void extractExprProps();

  std::unique_ptr<LookupContext> lookupCtx_;
  ExpressionProps exprProps_;
  std::vector<std::string> idxReturnCols_;
  std::vector<int32_t> schemaIds_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_LOOKUP_VALIDATOR_H_
