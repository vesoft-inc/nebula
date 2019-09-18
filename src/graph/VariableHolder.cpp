/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/VariableHolder.h"
#include "graph/InterimResult.h"

namespace nebula {
namespace graph {

VariableHolder::VariableHolder() {
}


VariableHolder::~VariableHolder() {
}


VariableHolder::VariableHolder(VariableHolder &&rhs) noexcept {
    holder_ = std::move(rhs.holder_);
}


VariableHolder& VariableHolder::operator=(VariableHolder &&rhs) noexcept {
    if (this == &rhs) {
        return *this;
    }
    holder_ = std::move(rhs.holder_);
    return *this;
}


void VariableHolder::add(const std::string &var, std::unique_ptr<InterimResult> result) {
    holder_[var] = std::move(result);
}


const InterimResult* VariableHolder::get(const std::string &var, bool *existing) const {
    auto iter = holder_.find(var);
    if (iter == holder_.end()) {
        if (existing != nullptr) {
            *existing = false;
        }
        return nullptr;
    }
    if (existing != nullptr) {
        *existing = true;
    }
    return iter->second.get();
}

}   // namespace graph
}   // namespace nebula
