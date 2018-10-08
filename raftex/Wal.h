/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_WAL_H_
#define RAFTEX_WAL_H_

#include "base/Base.h"
#include "raftex/LogIterator.h"

namespace vesoft {
namespace vgraph {
namespace raftex {

/**
 * Base class for all WAL implementations
 */
class Wal {
public:
    virtual ~Wal() = default;

    // Return the ID of the first log message in the WAL
    virtual LogID firstLogId() const = 0;

    // Return the ID of the last log message in the WAL
    virtual LogID lastLogId() const = 0;

    // Return the term to receive the last log
    virtual TermID lastLogTerm() const = 0;

    // Append one log message to the WAL
    virtual bool appendLog(LogID id, TermID term, std::string msg) = 0;

    // Append a list of log messages to the WAL
    virtual bool appendLogs(LogIterator& iter) = 0;

    // Rollback to the given id, all logs after the id will be discarded
    virtual bool rollbackToLog(LogID id) = 0;

    // Scan [firstLogId, lastLogId]
    virtual std::unique_ptr<LogIterator> iterator(int64_t firstLogId) = 0;
};

}  // namespace raftex
}  // namespace vgraph
}  // namespace vesoft
#endif  // RAFTEX_WAL_H_

