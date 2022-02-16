/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/QueryExpressionContext.h"

#include <ostream>  // for operator<<
#include <utility>  // for move

#include "common/base/Base.h"                // for UNUSED
#include "common/base/Logging.h"             // for LOG, LogMessage, _LOG_ERROR
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for Result

namespace nebula {
namespace graph {
const Value& QueryExpressionContext::getVar(const std::string& var) const {
  if (ectx_ == nullptr) {
    return Value::kEmpty;
  }
  return ectx_->getValue(var);
}

const Value& QueryExpressionContext::getVersionedVar(const std::string& var,
                                                     int64_t version) const {
  if (ectx_ == nullptr) {
    return Value::kEmpty;
  }
  return ectx_->getVersionedResult(var, version).value();
}

const Value& QueryExpressionContext::getVarProp(const std::string& var,
                                                const std::string& prop) const {
  UNUSED(var);
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getColumn(prop);
}

Value QueryExpressionContext::getTagProp(const std::string& tag, const std::string& prop) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getTagProp(tag, prop);
}

Value QueryExpressionContext::getEdgeProp(const std::string& edge, const std::string& prop) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getEdgeProp(edge, prop);
}

Value QueryExpressionContext::getSrcProp(const std::string& tag, const std::string& prop) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getTagProp(tag, prop);
}

const Value& QueryExpressionContext::getDstProp(const std::string& tag,
                                                const std::string& prop) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getTagProp(tag, prop);
}

const Value& QueryExpressionContext::getInputProp(const std::string& prop) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getColumn(prop);
}

Value QueryExpressionContext::getColumn(int32_t index) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getColumn(index);
}

Value QueryExpressionContext::getVertex(const std::string& name) const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getVertex(name);
}

Value QueryExpressionContext::getEdge() const {
  if (iter_ == nullptr) {
    return Value::kEmpty;
  }
  return iter_->getEdge();
}

void QueryExpressionContext::setVar(const std::string& var, Value val) {
  if (ectx_ == nullptr) {
    LOG(ERROR) << "Execution context was not provided.";
    return;
  }
  ectx_->setValue(var, std::move(val));
}

}  // namespace graph
}  // namespace nebula
