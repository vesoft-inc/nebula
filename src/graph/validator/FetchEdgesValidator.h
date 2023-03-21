/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
#define _VALIDATOR_FETCH_EDGES_VALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class FetchEdgesValidator final : public Validator {
 public:
  FetchEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateEdgeName();

  StatusOr<std::string> validateEdgeRef(const Expression* expr);

  Status validateEdgeKey();

  Status validateYield(const YieldClause* yieldClause);

  AstContext* getAstContext() override {
    return fetchCtx_.get();
  }

 private:
  std::string edgeName_;

  EdgeType edgeType_{0};

  std::shared_ptr<const meta::NebulaSchemaProvider> edgeSchema_;

  std::unique_ptr<FetchEdgesContext> fetchCtx_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
