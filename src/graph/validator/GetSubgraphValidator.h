/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_GETSUBGRAPHVALIDATOR_H_
#define GRAPH_VALIDATOR_GETSUBGRAPHVALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "parser/Clauses.h"

namespace nebula {
namespace graph {
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
