/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/RaftLogIterator.h"

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {
namespace raftex {

RaftLogIterator::RaftLogIterator(LogID firstLogId, std::vector<cpp2::RaftLogEntry> logEntries)
    : idx_(0), firstLogId_(firstLogId), logEntries_(std::move(logEntries)) {}

RaftLogIterator& RaftLogIterator::operator++() {
  ++idx_;
  return *this;
}

bool RaftLogIterator::valid() const {
  return idx_ < logEntries_.size();
}

LogID RaftLogIterator::logId() const {
  DCHECK(valid());
  return firstLogId_ + idx_;
}

TermID RaftLogIterator::logTerm() const {
  DCHECK(valid());
  return logEntries_.at(idx_).get_log_term();
}

ClusterID RaftLogIterator::logSource() const {
  DCHECK(valid());
  return logEntries_.at(idx_).get_cluster();
}

folly::StringPiece RaftLogIterator::logMsg() const {
  DCHECK(valid());
  return logEntries_.at(idx_).get_log_str();
}

}  // namespace raftex
}  // namespace nebula
