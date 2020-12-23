/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "kvstore/wal/test/InMemoryLogBuffer.h"

namespace nebula {
namespace wal {

void InMemoryLogBuffer::push(TermID term,
                             ClusterID cluster,
                             std::string&& msg) {
    folly::RWSpinLock::WriteHolder wh(&accessLock_);

    totalLen_ += msg.size()
                 + sizeof(TermID)
                 + sizeof(ClusterID)
                 + sizeof(LogID);
    logs_.emplace_back(term, cluster, std::move(msg));
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
    return std::get<0>(logs_.back());
}


TermID InMemoryLogBuffer::getTerm(size_t idx) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    CHECK_LT(idx, logs_.size());
    return std::get<0>(logs_[idx]);
}


ClusterID InMemoryLogBuffer::getCluster(size_t idx) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    CHECK_LT(idx, logs_.size());
    return std::get<1>(logs_[idx]);
}


const folly::StringPiece InMemoryLogBuffer::getLog(size_t idx) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    CHECK_LT(idx, logs_.size());
    return std::get<2>(logs_[idx]);
}


std::pair<LogID, TermID> InMemoryLogBuffer::accessAllLogs(
        std::function<void(LogID,
                           TermID,
                           ClusterID,
                           const std::string&)> fn) const {
    folly::RWSpinLock::ReadHolder rh(&accessLock_);
    LogID id = firstLogId_ - 1;
    TermID term = -1;
    for (auto& log : logs_) {
        ++id;
        term = std::get<0>(log);
        fn(id, term, std::get<1>(log), std::get<2>(log));
    }

    return std::make_pair(id, term);
}

}  // namespace wal
}  // namespace nebula

