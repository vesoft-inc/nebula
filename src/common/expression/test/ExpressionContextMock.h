/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/context/ExpressionContext.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"

namespace nebula {

class ExpressionContextMock final : public ExpressionContext {
 public:
  const Value& getVar(const std::string& var) const override {
    auto found = indices_.find(var);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      if (var == "versioned_var") {
        return vals_[found->second].getList().values.back();
      }
      return vals_[found->second];
    }
  }

  const Value& getVersionedVar(const std::string& var, int64_t version) const override {
    auto found = indices_.find(var);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      if (vals_[found->second].type() != Value::Type::LIST) {
        return Value::kNullValue;
      }
      auto size = vals_[found->second].getList().size();
      if (size <= static_cast<size_t>(std::abs(version))) {
        return Value::kNullValue;
      }
      if (version <= 0) {
        return vals_[found->second].getList()[std::abs(version)];
      } else {
        return vals_[found->second].getList()[size - version];
      }
    }
  }

  const Value& getVarProp(const std::string& var, const std::string& prop) const override {
    UNUSED(var);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  StatusOr<std::size_t> getVarPropIndex(const std::string& var,
                                        const std::string& prop) const override {
    UNUSED(var);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Status::Error("Not exists prop `%s'.", prop.c_str());
    }
    return found->second;
  }

  Value getEdgeProp(const std::string& edgeType, const std::string& prop) const override {
    UNUSED(edgeType);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  Value getTagProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  Value getSrcProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  const Value& getDstProp(const std::string& tag, const std::string& prop) const override {
    UNUSED(tag);
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  const Value& getInputProp(const std::string& prop) const override {
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Value::kNullValue;
    } else {
      return vals_[found->second];
    }
  }

  StatusOr<std::size_t> getInputPropIndex(const std::string& prop) const override {
    auto found = indices_.find(prop);
    if (found == indices_.end()) {
      return Status::Error("Not exists prop `%s'.", prop.c_str());
    }
    return found->second;
  }

  Value getVertex(const std::string& name = "") const override {
    UNUSED(name);
    return Value();
  }

  Value getEdge() const override {
    return Value();
  }

  const Value& getColumn(int32_t index) const override;

  void setVar(const std::string& var, Value val) override {
    // used by tests of list comprehesion, predicate or reduce
    if (var == "n" || var == "p" || var == "totalNum") {
      vals_.emplace_back(val);
      indices_[var] = vals_.size() - 1;
    }
  }

 private:
  static std::unordered_map<std::string, std::size_t> indices_;
  static std::vector<Value> vals_;
  std::unordered_map<std::string, std::regex> regex_;
};
}  // namespace nebula
