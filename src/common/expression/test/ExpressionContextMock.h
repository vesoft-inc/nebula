/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_EXPRESSION_TEST_EXPRESSIONCONTEXTMOCK_H
#define COMMON_EXPRESSION_TEST_EXPRESSIONCONTEXTMOCK_H
#include <stdint.h>  // for int32_t, int64_t
#include <stdlib.h>  // for abs, size_t

#include <cstdlib>        // for abs
#include <regex>          // for regex
#include <string>         // for string, operator==
#include <unordered_map>  // for operator==, _Node_iter...
#include <utility>        // for pair
#include <vector>         // for vector

#include "common/base/Base.h"                  // for UNUSED
#include "common/context/ExpressionContext.h"  // for ExpressionContext
#include "common/datatypes/List.h"             // for List
#include "common/datatypes/Value.h"            // for Value, Value::kNullValue

namespace nebula {

class ExpressionContextMock final : public ExpressionContext {
 public:
  const Value& getVar(const std::string& var) const override {
    auto found = vals_.find(var);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      if (var == "versioned_var") {
        return found->second.getList().values.back();
      }
      return found->second;
    }
  }

  const Value& getVersionedVar(const std::string& var, int64_t version) const override {
    auto found = vals_.find(var);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      if (found->second.type() != Value::Type::LIST) {
        return Value::kNullValue;
      }
      auto size = found->second.getList().size();
      if (size <= static_cast<size_t>(std::abs(version))) {
        return Value::kNullValue;
      }
      if (version <= 0) {
        return found->second.getList()[std::abs(version)];
      } else {
        return found->second.getList()[size - version];
      }
    }
  }

  const Value& getVarProp(const std::string& var, const std::string& prop) const override {
    UNUSED(var);
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  Value getEdgeProp(const std::string& edgeType, const std::string& prop) const override {
    UNUSED(edgeType);
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  Value getTagProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  Value getSrcProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  const Value& getDstProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  const Value& getInputProp(const std::string& prop) const override {
    auto found = vals_.find(prop);
    if (found == vals_.end()) {
      return Value::kNullValue;
    } else {
      return found->second;
    }
  }

  Value getVertex(const std::string& name = "") const override {
    UNUSED(name);
    return Value();
  }

  Value getEdge() const override {
    return Value();
  }

  Value getColumn(int32_t index) const override;

  void setVar(const std::string& var, Value val) override {
    // used by tests of list comprehension, predicate or reduce
    if (var == "n" || var == "p" || var == "totalNum") {
      vals_.erase(var);
      vals_[var] = val;
    }
  }

 private:
  static std::unordered_map<std::string, Value> vals_;
  std::unordered_map<std::string, std::regex> regex_;
};
}  // namespace nebula
#endif
