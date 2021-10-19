/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
#define _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_

#include "graph/context/ast/QueryAstContext.h"
#include "graph/validator/Validator.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class FetchVerticesValidator final : public Validator {
 public:
  FetchVerticesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateTag(const NameLabelList* nameLables);

  Status validateYield(YieldClause* yield);

  AstContext* getAstContext() override { return fetchCtx_.get(); }

  void extractVertexProp(ExpressionProps& exprProps);

 private:
  std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagsSchema_;

  std::unique_ptr<FetchVerticesContext> fetchCtx_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
