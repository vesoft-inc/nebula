/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/VariableHolder.h"
#include "graph/InterimResult.h"
#include "session/Session.h"

namespace nebula {
namespace graph {

void VariableHolder::add(const std::string &var,
                         std::unique_ptr<InterimResult> result, bool global) {
    if (global) {
        session_->globalVariableHolder()->add(var, std::move(result));
    } else {
        holder_[var] = std::move(result);
    }
}


const InterimResult* VariableHolder::get(const std::string &var, bool *existing) const {
    auto iter = holder_.find(var);
    if (iter != holder_.end()) {
        if (existing != nullptr) {
            *existing = true;
        }
        return iter->second.get();
    }
    auto interim = session_->globalVariableHolder()->get(var, existing);
    if (interim) {
        gHolder_.insert(interim);
    }
    return interim.get();
}

void GlobalVariableHolder::add(const std::string &var, std::unique_ptr<InterimResult> result) {
    folly::RWSpinLock::WriteHolder l(lock_);
    holder_[var] = std::shared_ptr<const graph::InterimResult>(result.release());
}

std::shared_ptr<const InterimResult>
GlobalVariableHolder::get(const std::string &var, bool *existing) const {
    folly::RWSpinLock::ReadHolder l(lock_);
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
    return iter->second;
}

}   // namespace graph
}   // namespace nebula
