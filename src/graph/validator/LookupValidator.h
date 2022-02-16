/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef _VALIDATOR_LOOKUP_VALIDATOR_H_
#define _VALIDATOR_LOOKUP_VALIDATOR_H_

#include <stdint.h>  // for int32_t

#include <memory>  // for shared_ptr, unique_ptr
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Status.h"              // for Status
#include "common/base/StatusOr.h"            // for StatusOr
#include "common/expression/Expression.h"    // for Expression, Expression...
#include "common/plugin/fulltext/FTUtils.h"  // for HttpClient
#include "common/plugin/fulltext/elasticsearch/ESGraphAdapter.h"
#include "common/thrift/ThriftTypes.h"         // for GraphSpaceID
#include "graph/validator/Validator.h"         // for Validator
#include "graph/visitor/DeducePropsVisitor.h"  // for ExpressionProps

namespace nebula {
class LogicalExpression;
class LookupSentence;
class RelationalExpression;
class Sentence;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph

class LogicalExpression;
class LookupSentence;
class RelationalExpression;
class Sentence;

namespace meta {
class NebulaSchemaProvider;
}  // namespace meta

namespace graph {

struct LookupContext;
class QueryContext;
struct AstContext;

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
  std::vector<::nebula::plugin::HttpClient> tsClients_;
  ExpressionProps exprProps_;
  std::vector<std::string> idxReturnCols_;
  std::vector<int32_t> schemaIds_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_LOOKUP_VALIDATOR_H_
