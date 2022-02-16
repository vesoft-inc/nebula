/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
#define _VALIDATOR_FETCH_EDGES_VALIDATOR_H_

#include <memory>  // for unique_ptr, shared_ptr
#include <string>  // for string

#include "common/base/Status.h"                 // for Status
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/datatypes/Value.h"             // for Value, Value::Type
#include "common/thrift/ThriftTypes.h"          // for EdgeType
#include "graph/context/ast/QueryAstContext.h"  // for FetchEdgesContext
#include "graph/validator/Validator.h"          // for Validator
#include "parser/TraverseSentences.h"

namespace nebula {
class Expression;
class Sentence;
class YieldClause;
namespace graph {
class QueryContext;
struct AstContext;
}  // namespace graph
namespace meta {
class SchemaProviderIf;
}  // namespace meta

class Expression;
class Sentence;
class YieldClause;
namespace meta {
class SchemaProviderIf;
}  // namespace meta

namespace graph {
class QueryContext;
struct AstContext;

class FetchEdgesValidator final : public Validator {
 public:
  FetchEdgesValidator(Sentence* sentence, QueryContext* context) : Validator(sentence, context) {}

 private:
  Status validateImpl() override;

  Status validateEdgeName();

  StatusOr<std::string> validateEdgeRef(const Expression* expr, Value::Type type);

  Status validateEdgeKey();

  Status validateYield(const YieldClause* yieldClause);

  AstContext* getAstContext() override {
    return fetchCtx_.get();
  }

 private:
  std::string edgeName_;

  EdgeType edgeType_{0};

  std::shared_ptr<const meta::SchemaProviderIf> edgeSchema_;

  std::unique_ptr<FetchEdgesContext> fetchCtx_;
};

}  // namespace graph
}  // namespace nebula

#endif  // _VALIDATOR_FETCH_EDGES_VALIDATOR_H_
