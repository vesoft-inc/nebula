/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_RAFTEX_RAFTLOGITERATOR_H
#define KVSTORE_RAFTEX_RAFTLOGITERATOR_H

#include <folly/Range.h>  // for StringPiece
#include <stddef.h>       // for size_t

#include <vector>  // for vector

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"        // for LogID, ClusterID, TermID
#include "common/utils/LogIterator.h"         // for LogIterator
#include "interface/gen-cpp2/raftex_types.h"  // for RaftLogEntry

namespace nebula {
namespace raftex {

/**
 * @brief The wal log iterator used when follower received logs from leader by rpc
 */
class RaftLogIterator final : public LogIterator {
 public:
  /**
   * @brief Construct a new raf log iterator
   *
   * @param firstLogId First log id in iterator
   * @param logEntries Log entries from rpc request
   */
  RaftLogIterator(LogID firstLogId, std::vector<cpp2::RaftLogEntry> logEntries);

  /**
   * @brief Move forward iterator to next log entry
   *
   * @return LogIterator&
   */
  RaftLogIterator& operator++() override;

  /**
   * @brief Return whether log iterator is valid
   */
  bool valid() const override;

  /**
   * @brief Return the log id pointed by current iterator
   */
  LogID logId() const override;

  /**
   * @brief Return the log term pointed by current iterator
   */
  TermID logTerm() const override;

  /**
   * @brief Return the log source pointed by current iterator
   */
  ClusterID logSource() const override;

  /**
   * @brief Return the log message pointed by current iterator
   */
  folly::StringPiece logMsg() const override;

 private:
  size_t idx_;
  const LogID firstLogId_;
  std::vector<cpp2::RaftLogEntry> logEntries_;
};

}  // namespace raftex
}  // namespace nebula
#endif
