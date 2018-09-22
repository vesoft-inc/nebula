/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "raftex/InMemoryLogBuffer.h"

namespace vesoft {
namespace vgraph {
namespace raftex {

void InMemoryLogBuffer::push(TermID term, std::string&& msg) {
    folly::RWSpinLock::WriteHolder wh(&accessLock_);

    totalLen_ += msg.size() + sizeof(TermID) + sizeof(LogID);
    logs_.emplace_back(std::make_pair(term, std::move(msg)));
}


size_t InMemoryLogBuffer::size() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return totalLen_;
}


size_t InMemoryLogBuffer::numLogs() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return logs_.size();
}


bool InMemoryLogBuffer::empty() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return logs_.empty();
}


LogID InMemoryLogBuffer::firstLogId() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return firstLogId_;
}


LogID InMemoryLogBuffer::lastLogId() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return logs_.size() + firstLogId_ - 1;
}


TermID InMemoryLogBuffer::lastLogTerm() const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    return logs_.back().first;
}


TermID InMemoryLogBuffer::getTerm(size_t idx) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    CHECK_LT(idx, logs_.size());
    return logs_[idx].first;
}


folly::StringPiece InMemoryLogBuffer::getLog(size_t idx) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    CHECK_LT(idx, logs_.size());
    return logs_[idx].second;
}


void InMemoryLogBuffer::freeze() {
    frozen_ = true;
}


bool InMemoryLogBuffer::isFrozen() const {
    return frozen_;
}


void InMemoryLogBuffer::rollover() {
    rollover_ = true;
}


bool InMemoryLogBuffer::needToRollover() const {
    return rollover_;
}


std::pair<LogID, TermID> InMemoryLogBuffer::accessAllLogs(
        std::function<void (LogID, TermID, const std::string&)> fn) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    LogID id = firstLogId_ - 1;
    TermID term = -1;
    for (auto& log : logs_) {
        ++id;
        term = log.first;
        fn(id, term, log.second);
    }

    return std::make_pair(id, term);
}

}  // namespace raftex
}  // namespace vgraph
}  // namespace vesoft

