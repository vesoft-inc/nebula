/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Value.h"

#include "context/QueryExpressionContext.h"

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
    if (iter_ != nullptr) {
        return iter_->getColumn(prop);
    } else {
        return Value::kEmpty;
    }
}

Value QueryExpressionContext::getTagProp(const std::string& tag,
                    const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getTagProp(tag, prop);
    } else {
        return Value::kEmpty;
    }
}

Value QueryExpressionContext::getEdgeProp(const std::string& edge,
                                         const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getEdgeProp(edge, prop);
    } else {
        return Value::kEmpty;
    }
}

Value QueryExpressionContext::getSrcProp(const std::string& tag,
                                        const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getTagProp(tag, prop);
    } else {
        return Value::kEmpty;
    }
}

const Value& QueryExpressionContext::getDstProp(const std::string& tag,
                                               const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getTagProp(tag, prop);
    } else {
        return Value::kEmpty;
    }
}

const Value& QueryExpressionContext::getInputProp(const std::string& prop) const {
    if (iter_ != nullptr) {
        return iter_->getColumn(prop);
    } else {
        return Value::kEmpty;
    }
}

Value QueryExpressionContext::getVertex() const {
    if (iter_ != nullptr) {
        return iter_->getVertex();
    } else {
        return Value::kEmpty;
    }
}

Value QueryExpressionContext::getEdge() const {
    if (iter_ != nullptr) {
        return iter_->getEdge();
    } else {
        return Value::kEmpty;
    }
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
