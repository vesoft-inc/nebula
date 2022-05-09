/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_UTILS_DEFAULTVALUECONTEXT_H_
#define COMMON_UTILS_DEFAULTVALUECONTEXT_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "common/datatypes/Value.h"

namespace nebula {
class DefaultValueContext final : public ExpressionContext {
 public:
  const Value& getVar(const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  const Value& getVersionedVar(const std::string&, int64_t) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  const Value& getVarProp(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  StatusOr<std::size_t> getVarPropIndex(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Status::Error("Unimplemented");
  }

  Value getEdgeProp(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  Value getTagProp(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  Value getSrcProp(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  const Value& getDstProp(const std::string&, const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  const Value& getInputProp(const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  StatusOr<std::size_t> getInputPropIndex(const std::string&) const override {
    LOG(FATAL) << "Not allowed to call";
    return Status::Error("Unimplemented");
  }

  const Value& getColumn(int32_t) const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  void setVar(const std::string&, Value) override {}

  Value getVertex(const std::string& name = "") const override {
    UNUSED(name);
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }

  Value getEdge() const override {
    LOG(FATAL) << "Not allowed to call";
    return Value::kEmpty;
  }
};
}  // namespace nebula

#endif  // COMMON_UTILS_DEFAULTVALUECONTEXT_H_
