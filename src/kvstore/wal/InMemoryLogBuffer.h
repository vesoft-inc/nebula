/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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

private:
    mutable folly::RWSpinLock accessLock_;

    std::vector<std::tuple<TermID, ClusterID, std::string>> logs_;
    LogID firstLogId_{-1};
    size_t totalLen_{0};
};


using BufferPtr = std::shared_ptr<InMemoryLogBuffer>;
using BufferList = std::list<BufferPtr>;

}  // namespace wal
}  // namespace nebula

#endif  // WAL_INMEMORYLOGBUFFER_H_
