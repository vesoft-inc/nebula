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
        logs_.reserve(1 << FLAGS_wal_buffer_size_exp);
        mask_ = logs_.capacity() - 1;
    }


    // Push a new message to the end of the buffer
    void push(LogID id, TermID term, ClusterID cluster, std::string&& msg) {
        folly::RWSpinLock::WriteHolder wh(&accessLock_);
        if (logs_.empty()) {
            firstLogId_ = id;
        }

        if (logs_.size() < logs_.capacity()) {
            logs_.emplace_back(id, term, cluster, std::move(msg));
        } else {
            ++firstLogId_;
            ++firstLogIdIndex_;
            logs_[firstLogIdIndex_ - 1] = {id, term, cluster, std::move(msg)};
            if (firstLogIdIndex_ == logs_.capacity()) {
                firstLogIdIndex_ = 0;
            }
        }
    }

    LogID firstLogId() const {
        folly::RWSpinLock::ReadHolder rh(&accessLock_);
        return firstLogId_;
    }

    bool getLogEntry(LogID logId, LogEntry& logEntry) {
        folly::RWSpinLock::ReadHolder rh(&accessLock_);
        if (logs_.empty() || logId < firstLogId_ ||
            logId >= firstLogId_ + static_cast<int64_t>(logs_.size())) {
            return false;
        }
        size_t idx = (firstLogIdIndex_ + (logId - firstLogId_)) & mask_;
        logEntry = logs_[idx];
        return true;
    }

    void clear() {
        folly::RWSpinLock::WriteHolder wh(&accessLock_);
        logs_.clear();
        firstLogId_ = std::numeric_limits<LogID>::max();
        firstLogIdIndex_ = 0;
    }

private:
    mutable folly::RWSpinLock accessLock_;
    std::vector<std::tuple<LogID, TermID, ClusterID, std::string>> logs_;
    LogID firstLogId_{std::numeric_limits<LogID>::max()};
    size_t firstLogIdIndex_{0};
    size_t mask_;
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_INMEMORYLOGBUFFER_H_
