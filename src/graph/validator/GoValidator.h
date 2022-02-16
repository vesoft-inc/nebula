/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_GOVALIDATOR_H_
#define GRAPH_VALIDATOR_GOVALIDATOR_H_

#include <memory>         // for unique_ptr
#include <string>         // for hash, string
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <vector>         // for vector

#include "common/base/Status.h"                 // for Status
#include "common/thrift/ThriftTypes.h"          // for TagID
#include "graph/context/ast/QueryAstContext.h"  // for GoContext
#include "graph/planner/plan/Query.h"
#include "graph/validator/Validator.h"         // for Validator
#include "interface/gen-cpp2/storage_types.h"  // for EdgeProp, VertexProp

namespace nebula {
class Expression;
class Sentence;
class TruncateClause;
class WhereClause;
class YieldClause;
class YieldColumn;
class YieldColumns;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph

class Expression;
class Sentence;
class TruncateClause;
class WhereClause;
class YieldClause;
class YieldColumn;
class YieldColumns;

namespace graph {
class QueryContext;
struct AstContext;

class GoValidator final : public Validator {
 public:
  using VertexProp = ::nebula::storage::cpp2::VertexProp;
  using EdgeProp = ::nebula::storage::cpp2::EdgeProp;

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

  void extractPropExprs(const Expression* expr, std::unordered_set<std::string>& uniqueCols);

  Expression* rewrite2VarProp(const Expression* expr);

 private:
  std::unique_ptr<GoContext> goCtx_;

  YieldColumns* inputPropCols_{nullptr};
  std::unordered_map<std::string, YieldColumn*> propExprColMap_;
  std::vector<TagID> tagIds_;
};
}  // namespace graph
}  // namespace nebula
#endif
