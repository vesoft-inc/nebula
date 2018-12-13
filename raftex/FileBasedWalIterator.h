/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_FILEBASEDWALITERATOR_H_
#define RAFTEX_FILEBASEDWALITERATOR_H_

#include "base/Base.h"
#include "raftex/LogIterator.h"
#include "raftex/InMemoryLogBuffer.h"

namespace vesoft {
namespace raftex {

class FileBasedWal;


/**
 * Log message iterator
 *
 * The iterator tries to find the given log message either in wal files,
 * or from the in-memory buffers. If the given log id is out of range,
 * an invalid (valid() method will return false) iterator will be
 * constructed
 */
class FileBasedWalIterator final : public LogIterator {
public:
    // The range is [startId, lastId]
    // if the lastId < 0, the wal_->lastId_ will be used
    FileBasedWalIterator(std::shared_ptr<FileBasedWal> wal,
                         LogID startId,
                         LogID lastId = -1);
    virtual ~FileBasedWalIterator();

    LogIterator& operator++() override;

    bool valid() const override;

    LogID logId() const override;

    TermID logTerm() const override;

    ClusterID logSource() const override;

    folly::StringPiece logMsg() const override;

private:
    LogID getFirstIdInNextBuffer() const;
    LogID getFirstIdInNextFile() const;

private:
    // Holds the Wal object, so that it will not be destroyed before the iterator
    std::shared_ptr<FileBasedWal> wal_;

    LogID lastId_;
    LogID currId_;
    TermID currTerm_;
    LogID firstIdInBuffer_{std::numeric_limits<LogID>::max()};

    // First id in next buffer or in next WAL file
    LogID nextFirstId_;

    BufferList buffers_;
    size_t currIdx_{0};

    // [firstId, lastId]
    std::list<std::pair<LogID, LogID>> idRanges_;
    std::list<int> fds_;
    int64_t currPos_{0};
    int32_t currMsgLen_{0};
    mutable std::string currLog_;
};

}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_FILEBASEDWALITERATOR_H_
