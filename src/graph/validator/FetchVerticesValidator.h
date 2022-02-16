/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
#define _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_

#include <map>     // for map
#include <memory>  // for shared_ptr, unique_ptr
#include <vector>  // for vector

#include "common/base/Status.h"                 // for Status
#include "common/thrift/ThriftTypes.h"          // for TagID
#include "graph/context/ast/QueryAstContext.h"  // for FetchVerticesContext
#include "graph/validator/Validator.h"          // for Validator
#include "parser/TraverseSentences.h"

namespace nebula {
class NameLabelList;
class Sentence;
class YieldClause;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph
namespace meta {
class SchemaProviderIf;
}  // namespace meta

class NameLabelList;
class Sentence;
class YieldClause;
namespace meta {
class SchemaProviderIf;
}  // namespace meta

namespace graph {
class QueryContext;
struct AstContext;

class FetchVerticesValidator final : public Validator {
 public:
  FetchVerticesValidator(Sentence* sentence, QueryContext* context)
      : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateTag(const NameLabelList* nameLabels);

  Status validateYield(YieldClause* yield);

  AstContext* getAstContext() override {
    return fetchCtx_.get();
  }

 private:
  std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagsSchema_;
  std::vector<TagID> tagIds_;

  std::unique_ptr<FetchVerticesContext> fetchCtx_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_FETCH_VERTICES_VALIDATOR_H_
