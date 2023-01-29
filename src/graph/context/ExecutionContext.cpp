/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/ExecutionContext.h"

#include "graph/gc/GC.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {
constexpr int64_t ExecutionContext::kLatestVersion;
constexpr int64_t ExecutionContext::kOldestVersion;
constexpr int64_t ExecutionContext::kPreviousOneVersion;

void ExecutionContext::setValue(const std::string& name, Value&& val) {
  ResultBuilder builder;
  builder.value(std::move(val)).iter(Iterator::Kind::kDefault);
  setResult(name, builder.build());
}

void ExecutionContext::setResult(const std::string& name, Result&& result) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto& hist = valueMap_[name];
  hist.emplace_back(std::move(result));
}

void ExecutionContext::dropResult(const std::string& name) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  if (valueMap_.count(name) == 0) {
    return;
  }
  auto& val = valueMap_[name];
  if (FLAGS_enable_async_gc) {
    GC::instance().clear(std::move(val));
  } else {
    val.clear();
  }
}

size_t ExecutionContext::numVersions(const std::string& name) const {
  folly::RWSpinLock::ReadHolder holder(lock_);
  auto it = valueMap_.find(name);
  CHECK(it != valueMap_.end());
  return it->second.size();
}

// Only keep the last several versions of the Value
void ExecutionContext::truncHistory(const std::string& name, size_t numVersionsToKeep) {
  folly::RWSpinLock::WriteHolder holder(lock_);
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
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto it = valueMap_.find(name);
  if (it != valueMap_.end() && !it->second.empty()) {
    return it->second.back().moveValue();
  } else {
    return Value();
  }
}

const Result& ExecutionContext::getResult(const std::string& name) const {
  folly::RWSpinLock::ReadHolder holder(lock_);
  auto it = valueMap_.find(name);
  if (it != valueMap_.end() && !it->second.empty()) {
    return it->second.back();
  } else {
    return Result::EmptyResult();
  }
}

void ExecutionContext::setVersionedResult(const std::string& name,
                                          Result&& result,
                                          int64_t version) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto it = valueMap_.find(name);
  if (it != valueMap_.end()) {
    auto& hist = it->second;
    auto size = hist.size();
    if (static_cast<size_t>(std::abs(version)) >= size) {
      return;
    }
    hist[(size + version - 1) % size] = std::move(result);
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
  folly::RWSpinLock::ReadHolder holder(lock_);
  auto it = valueMap_.find(name);
  if (it != valueMap_.end()) {
    return it->second;
  } else {
    return Result::EmptyResultList();
  }
}

}  // namespace graph
}  // namespace nebula
