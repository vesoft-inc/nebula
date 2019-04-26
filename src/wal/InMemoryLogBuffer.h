/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WAL_INMEMORYLOGBUFFER_H_
#define WAL_INMEMORYLOGBUFFER_H_

#include "base/Base.h"

namespace nebula {
namespace wal {

//
// In-memory buffer (thread-safe)
//
class InMemoryLogBuffer final {
public:
    explicit InMemoryLogBuffer(LogID firstLogId)
        : firstLogId_(firstLogId) {}

    // Push a new message to the end of the buffer
    void push(TermID term, ClusterID cluster, std::string&& msg);

    size_t size() const;
    size_t numLogs() const;
    bool empty() const;

    LogID firstLogId() const;
    LogID lastLogId() const;
    TermID lastLogTerm() const;

    TermID getTerm(size_t idx) const;
    ClusterID getCluster(size_t idx) const;
    // The returned StringPiece value is valid as long as this
    // InMemoryLogBuffer object is alive. So please make sure
    // the returned StringPiece object will not outlive this buffer
    // object
    const folly::StringPiece getLog(size_t idx) const;

    // Iterates through all logs and calls the given functor fn
    // for each log
    // Returns the LogID and TermID for the last log
    std::pair<LogID, TermID> accessAllLogs(
        std::function<void(LogID,
                           TermID,
                           ClusterID,
                           const std::string&)> fn) const;

    // Mark the buffer ready for persistence
    bool freeze();

    void rollover();
    bool needToRollover() const;

    bool invalid() const;
    void markInvalid();

private:
    mutable folly::RWSpinLock accessLock_;

    std::vector<std::tuple<TermID, ClusterID, std::string>> logs_;
    LogID firstLogId_{-1};
    size_t totalLen_{0};

    // If this is true, the previous wal file will be closed first.
    // A new file will be created for this buffer
    bool rollover_{false};

    // When a buffer is frozen, no further write will be allowed.
    // It's ready to be flushed out
    std::atomic<bool> frozen_{false};

    // If the buffer marked invalid, it means we should not flush it.
    bool invalid_{false};
};


using BufferPtr = std::shared_ptr<InMemoryLogBuffer>;
using BufferList = std::list<BufferPtr>;

}  // namespace wal
}  // namespace nebula

#endif  // WAL_INMEMORYLOGBUFFER_H_
