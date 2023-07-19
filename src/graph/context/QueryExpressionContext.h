/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_QUERYEXPRESSIONCONTEXT_H_
#define GRAPH_CONTEXT_QUERYEXPRESSIONCONTEXT_H_

#include <cstddef>

#include "common/context/ExpressionContext.h"
#include "graph/context/ExecutionContext.h"
#include "graph/context/Iterator.h"

namespace nebula {
namespace graph {

class QueryExpressionContext final : public ExpressionContext {
 public:
  explicit QueryExpressionContext(ExecutionContext* ectx = nullptr) {
    ectx_ = ectx;
  }

  // Get the latest version value for the given variable name, such as $a, $b
  const Value& getVar(const std::string& var) const override;

  // Set the value of innerVar. The innerVar is a variable defined in an expression.
  // e.g. ListComprehension
  void setInnerVar(const std::string& var, Value val) override;

  // Get the value of innerVar
  const Value& getInnerVar(const std::string& var) const override;

  // Get the given version value for the given variable name, such as $a, $b
  const Value& getVersionedVar(const std::string& var, int64_t version) const override;

  // Get the specified property from a variable, such as $a.prop_name
  const Value& getVarProp(const std::string& var, const std::string& prop) const override;

  // Get index of property index in variable tuple
  StatusOr<std::size_t> getVarPropIndex(const std::string& var,
                                        const std::string& prop) const override;

  // Get the specified property from the tag, such as tag.prop_name
  Value getTagProp(const std::string& tag, const std::string& prop) const override;

  // Get the specified property from the edge, such as edge_type.prop_name
  Value getEdgeProp(const std::string& edge, const std::string& prop) const override;

  // Get the specified property from the source vertex, such as $^.prop_name
  Value getSrcProp(const std::string& tag, const std::string& prop) const override;

  // Get the specified property from the destination vertex, such as
  // $$.prop_name
  const Value& getDstProp(const std::string& tag, const std::string& prop) const override;

  // Get the specified property from the input, such as $-.prop_name
  const Value& getInputProp(const std::string& prop) const override;

  // Get index of property index in input tuple
  StatusOr<std::size_t> getInputPropIndex(const std::string& prop) const override;

  // Get the value by column index
  const Value& getColumn(int32_t index) const override;

  // Get Vertex
  Value getVertex(const std::string& name = "") const override;

  // Get Edge
  Value getEdge() const override;

  void setVar(const std::string&, Value val) override;

  QueryExpressionContext& operator()(Iterator* iter = nullptr) {
    iter_ = iter;
    return *this;
  }

 private:
  // ExecutionContext and Iterator are used for getting runtime results,
  // and nullptr is acceptable for these two members if the expressions
  // could be evaluated as constant value.
  ExecutionContext* ectx_{nullptr};
  Iterator* iter_{nullptr};

  // Expression value map that stores the value of innerVar
  std::unordered_map<std::string, Value> exprValueMap_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_CONTEXT_QUERYEXPRESSIONCONTEXT_H_
