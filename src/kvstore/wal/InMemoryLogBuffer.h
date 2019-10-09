/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_INMEMORYLOGBUFFER_H_
#define WAL_INMEMORYLOGBUFFER_H_

#include "base/Base.h"

DECLARE_int32(wal_buffer_size_exp);

using nebula::LogEntry;

namespace nebula {
namespace wal {

class InMemoryLogBuffer final {
public:
    InMemoryLogBuffer() {
        CHECK_GT(FLAGS_wal_buffer_size_exp, 0);
        CHECK_LT(FLAGS_wal_buffer_size_exp, 15);
        logs_.resize(1UL << FLAGS_wal_buffer_size_exp);
        mask_ = logs_.size() - 1;
        empty_ = true;
        full_ = false;
    }


    // Push a new message to the end of the buffer
    void push(LogID id, TermID term, ClusterID cluster, std::string&& msg) {
        folly::RWSpinLock::WriteHolder wh(&accessLock_);
        if (empty_) {
            firstLogId_ = id;
            lastLogId_ = id - 1;
            empty_ = false;
        }
        if (full_) {
            ++firstLogId_;
            ++firstLogIndex_;
        }
        ++lastLogId_;
        logs_[nextLogIndex_++] = {id, term, cluster, std::move(msg)};
        if (nextLogIndex_ == logs_.size()) {
            firstLogIndex_ = 0;
            nextLogIndex_ = 0;
            full_ = true;
        }
    }

    LogID firstLogId() const {
        folly::RWSpinLock::ReadHolder rh(&accessLock_);
        return firstLogId_;
    }

    bool getLogEntry(LogID logId, LogEntry& logEntry) {
        folly::RWSpinLock::ReadHolder rh(&accessLock_);
        if (empty_ || logId < firstLogId_ || logId > lastLogId_) {
            return false;
        }
        size_t idx = (firstLogIndex_ + (logId - firstLogId_)) & mask_;
        logEntry = logs_[idx];
        return true;
    }

    void clear() {
        folly::RWSpinLock::WriteHolder wh(&accessLock_);
        logs_.clear();
        logs_.resize(1UL << FLAGS_wal_buffer_size_exp);
        firstLogId_ = std::numeric_limits<LogID>::max();
        lastLogId_ = std::numeric_limits<LogID>::max();
        firstLogIndex_ = 0;
        nextLogIndex_ = 0;
        empty_ = true;
        full_ = false;
    }

private:
    mutable folly::RWSpinLock accessLock_;
    std::vector<LogEntry> logs_;
    LogID firstLogId_{std::numeric_limits<LogID>::max()};
    LogID lastLogId_{std::numeric_limits<LogID>::max()};
    size_t firstLogIndex_{0};
    size_t nextLogIndex_{0};
    size_t mask_;
    bool empty_;
    bool full_;
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_INMEMORYLOGBUFFER_H_
