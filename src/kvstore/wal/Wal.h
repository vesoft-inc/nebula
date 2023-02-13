/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WAL_WAL_H_
#define WAL_WAL_H_

#include "common/base/Base.h"
#include "common/utils/LogIterator.h"

namespace nebula {
namespace wal {

/**
 * Base class for all WAL implementations
 */
class Wal {
 public:
  virtual ~Wal() = default;

  /**
   * @brief Return the ID of the first log message in the WAL
   */
  virtual LogID firstLogId() const = 0;

  /**
   * @brief Return the ID of the last log message in the WAL
   */
  virtual LogID lastLogId() const = 0;

  /**
   * @brief Return the term of the last log message in the WAL
   */
  virtual TermID lastLogTerm() const = 0;

  /**
   * @brief Return the term of specified logId, if not exist，return -1
   */
  virtual TermID getLogTerm(LogID id) = 0;

  /**
   * @brief Append one log message to the WAL
   *
   * @param id Log id to append
   * @param term Log term to append
   * @param cluster Cluster id in log to append
   * @param msg Log messgage to append
   * @return  Whether append succeed
   */
  virtual bool appendLog(LogID id, TermID term, ClusterID cluster, std::string msg) = 0;

  // Append a list of log messages to the WAL
  /**
   * @brief Append a list of log messages to the WAL.
   *
   * @param iter Log iterator to append
   * @return  Whether append succeed
   */
  virtual bool appendLogs(LogIterator& iter) = 0;

  /**
   * @brief Rollback to the given id, all logs after the id will be discarded
   *
   * @param id The log id to rollback
   * @return Whether rollback succeed
   */
  virtual bool rollbackToLog(LogID id) = 0;

  /**
   * @brief Hard link the wal files to a new path
   *
   * @param newPath New wal path
   * @return Whether link succeed
   */
  virtual bool linkCurrentWAL(const char* newPath) = 0;

  /**
   * @brief Reset the WAL. This method is *NOT* thread safe
   *
   * @return Whether reset succeed
   */
  virtual bool reset() = 0;

  /**
   * @brief Clean time expired wal by ttl
   */
  virtual void cleanWAL() = 0;

  /**
   * @brief Clean wal by given log id
   */
  virtual void cleanWAL(LogID id) = 0;

  /**
   * @brief Scan the wal in range [firstLogId, lastLogId].
   *
   * @param firstLogId Start log id, inclusive
   * @param lastLogId End log id, inclusive
   * @return std::unique_ptr<LogIterator>
   */
  virtual std::unique_ptr<LogIterator> iterator(LogID firstLogId, LogID lastLogId) = 0;
};

}  // namespace wal
}  // namespace nebula
#endif  // WAL_WAL_H_
