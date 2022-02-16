/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_FINDPATHVALIDATOR_H_
#define GRAPH_VALIDATOR_FINDPATHVALIDATOR_H_

#include <memory>  // for unique_ptr

#include "common/base/Status.h"                 // for Status
#include "graph/context/ast/QueryAstContext.h"  // for PathContext
#include "graph/validator/Validator.h"          // for Validator

namespace nebula {
class Sentence;
class WhereClause;
class YieldClause;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph

class Sentence;
class WhereClause;
class YieldClause;

namespace graph {
class QueryContext;
struct AstContext;

class FindPathValidator final : public Validator {
 public:
  FindPathValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  AstContext* getAstContext() override {
    return pathCtx_.get();
  }

  Status validateWhere(WhereClause* where);

  Status validateYield(YieldClause* yield);

 private:
  std::unique_ptr<PathContext> pathCtx_;
};
}  // namespace graph
}  // namespace nebula
#endif
