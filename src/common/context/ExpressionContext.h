/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_CONTEXT_EXPRESSIONCONTEXT_H_
#define COMMON_CONTEXT_EXPRESSIONCONTEXT_H_

#include <folly/RWSpinLock.h>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"

namespace nebula {

/***************************************************************************
 *
 * The base class for all ExpressionContext implementations
 *
 * The context is NOT thread-safe
 *
 **************************************************************************/
class ExpressionContext {
 public:
  virtual ~ExpressionContext() = default;

  // Get the latest version value for the given variable name, such as $a, $b
  virtual const Value& getVar(const std::string& var) const = 0;

  // Set the value of innerVar. The innerVar is a variable defined in an expression.
  // e.g. ListComprehension
  virtual void setInnerVar(const std::string& var, Value val) = 0;

  // Get the value of innerVar.
  virtual const Value& getInnerVar(const std::string& var) const = 0;

  // Get the given version value for the given variable name, such as $a, $b
  virtual const Value& getVersionedVar(const std::string& var, int64_t version) const = 0;

  // Get the specified property from a variable, such as $a.prop_name
  virtual const Value& getVarProp(const std::string& var, const std::string& prop) const = 0;

  // Get index of variable property in tuple
  virtual StatusOr<std::size_t> getVarPropIndex(const std::string& var,
                                                const std::string& prop) const = 0;

  // Get the specified property from the edge, such as edge_type.prop_name
  virtual Value getEdgeProp(const std::string& edgeType, const std::string& prop) const = 0;

  // Get the specified property from the tag, such as tag.prop_name
  virtual Value getTagProp(const std::string& tag, const std::string& prop) const = 0;

  // Get the specified property from the source vertex, such as
  // $^.tag_name.prop_name
  virtual Value getSrcProp(const std::string& tag, const std::string& prop) const = 0;

  // Get the specified property from the destination vertex, such as
  // $$.tag_name.prop_name
  virtual const Value& getDstProp(const std::string& tag, const std::string& prop) const = 0;

  // Get the specified property from the input, such as $-.prop_name
  virtual const Value& getInputProp(const std::string& prop) const = 0;

  // Get index of input property in tuple
  virtual StatusOr<std::size_t> getInputPropIndex(const std::string& prop) const = 0;

  // Get Vertex
  virtual Value getVertex(const std::string& name = "") const = 0;

  // Get Edge
  virtual Value getEdge() const = 0;

  // Get Value by Column index
  virtual const Value& getColumn(int32_t index) const = 0;

  // Get regex
  const std::regex& getRegex(const std::string& pattern) {
    auto iter = regex_.find(pattern);
    if (iter == regex_.end()) {
      iter = regex_.emplace(pattern, std::regex(pattern)).first;
    }
    return iter->second;
  }

  virtual void setVar(const std::string& var, Value val) = 0;

 private:
  std::unordered_map<std::string, std::regex> regex_;
};

}  // namespace nebula
#endif  // COMMON_CONTEXT_EXPRESSIONCONTEXT_H_
