/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/ExecutionContext.h"

namespace nebula {
namespace graph {
constexpr int64_t ExecutionContext::kLatestVersion;
constexpr int64_t ExecutionContext::kOldestVersion;
constexpr int64_t ExecutionContext::kPreviousOneVersion;

void ExecutionContext::setValue(const std::string& name, Value&& val) {
    ResultBuilder builder;
    builder.value(std::move(val)).iter(Iterator::Kind::kDefault);
    setResult(name, builder.finish());
}

void ExecutionContext::setResult(const std::string& name, Result&& result) {
    auto& hist = valueMap_[name];
    hist.emplace_back(std::move(result));
}

void ExecutionContext::dropResult(const std::string& name) {
    valueMap_[name].clear();
}

size_t ExecutionContext::numVersions(const std::string& name) const {
    auto it = valueMap_.find(name);
    CHECK(it != valueMap_.end());
    return it->second.size();
}

// Only keep the last several versoins of the Value
void ExecutionContext::truncHistory(const std::string& name, size_t numVersionsToKeep) {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        if (it->second.size() <= numVersionsToKeep) {
            return;
        }
        // Only keep the latest N values
        it->second.erase(it->second.begin(), it->second.end() - numVersionsToKeep);
    }
}

// Get the latest version of the value
const Value& ExecutionContext::getValue(const std::string& name) const {
    return getResult(name).value();
}

Value ExecutionContext::moveValue(const std::string& name) {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end() && !it->second.empty()) {
        return it->second.back().moveValue();
    } else {
        return Value();
    }
}

const Result& ExecutionContext::getResult(const std::string& name) const {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end() && !it->second.empty()) {
        return it->second.back();
    } else {
        return Result::EmptyResult();
    }
}

const Result& ExecutionContext::getVersionedResult(const std::string& name, int64_t version) const {
    auto& result = getHistory(name);
    auto size = result.size();
    if (static_cast<size_t>(std::abs(version)) >= size) {
        return Result::EmptyResult();
    } else {
        return result[(size + version - 1) % size];
    }
}

const std::vector<Result>& ExecutionContext::getHistory(const std::string& name) const {
    auto it = valueMap_.find(name);
    if (it != valueMap_.end()) {
        return it->second;
    } else {
        return Result::EmptyResultList();
    }
}

}   // namespace graph
}   // namespace nebula
