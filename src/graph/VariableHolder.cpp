/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/VariableHolder.h"
#include "graph/IntermResult.h"

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


void VariableHolder::add(const std::string &var, std::unique_ptr<IntermResult> result) {
    holder_[var] = std::move(result);
}


const IntermResult* VariableHolder::get(const std::string &var) const {
    auto iter = holder_.find(var);
    if (iter == holder_.end()) {
        return nullptr;
    }
    return iter->second.get();
}

}   // namespace graph
}   // namespace nebula
