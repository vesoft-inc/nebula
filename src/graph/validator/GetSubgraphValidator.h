/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define GRAPH_VALIDATOR_GETSUBGRAPHVALIDATOR_H_

#include <memory>  // for unique_ptr

#include "common/base/Status.h"                 // for Status
#include "graph/context/ast/QueryAstContext.h"  // for SubgraphContext
#include "graph/validator/Validator.h"          // for Validator
#include "parser/Clauses.h"                     // for BothInOutClause, InBo...

namespace nebula {
class Sentence;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph

class Sentence;

namespace graph {
class QueryContext;
struct AstContext;

class GetSubgraphValidator final : public Validator {
 public:
  GetSubgraphValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateInBound(InBoundClause* in);

  Status validateOutBound(OutBoundClause* out);

  Status validateBothInOutBound(BothInOutClause* out);

  Status validateYield(YieldClause* yield);

  AstContext* getAstContext() override {
    return subgraphCtx_.get();
  }

 private:
  std::unique_ptr<SubgraphContext> subgraphCtx_;
};

}  // namespace graph
}  // namespace nebula
#endif
