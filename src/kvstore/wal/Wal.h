/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_WAL_H_
#define WAL_WAL_H_

#include "base/Base.h"
#include "base/LogIterator.h"

namespace nebula {
namespace wal {

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
    virtual bool appendLog(LogID id,
                           TermID term,
                           ClusterID cluster,
                           std::string msg) = 0;

    // Append a list of log messages to the WAL
    virtual bool appendLogs(LogIterator& iter) = 0;

    // Rollback to the given id, all logs after the id will be discarded
    virtual bool rollbackToLog(LogID id) = 0;

    // Clean all wal files
    virtual bool reset() = 0;

    // Scan [firstLogId, lastLogId]
    virtual std::unique_ptr<LogIterator> iterator(LogID firstLogId,
                                                  LogID lastLogId) = 0;
};

}  // namespace wal
}  // namespace nebula
#endif  // WAL_WAL_H_

