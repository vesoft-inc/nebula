/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_VALIDATOR_FINDPATHVALIDATOR_H_
#define GRAPH_VALIDATOR_FINDPATHVALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"

namespace nebula {
namespace graph {

class FindPathValidator final : public Validator {
 public:
  FindPathValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  AstContext* getAstContext() override { return pathCtx_.get(); }

  Status validateWhere(WhereClause* where);

 private:
  std::unique_ptr<PathContext> pathCtx_;
};
}  // namespace graph
}  // namespace nebula
#endif
