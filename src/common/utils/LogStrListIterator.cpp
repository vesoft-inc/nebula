/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/utils/LogStrListIterator.h"

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {

LogStrListIterator::LogStrListIterator(LogID firstLogId,
                                       TermID term,
                                       std::vector<cpp2::LogEntry> logEntries)
    : firstLogId_(firstLogId), term_(term), logEntries_(std::move(logEntries)) {
  idx_ = 0;
}

LogIterator& LogStrListIterator::operator++() {
  ++idx_;
  return *this;
}

bool LogStrListIterator::valid() const {
  return idx_ < logEntries_.size();
}

LogID LogStrListIterator::logId() const {
  DCHECK(valid());
  return firstLogId_ + idx_;
}

TermID LogStrListIterator::logTerm() const {
  DCHECK(valid());
  return term_;
}

ClusterID LogStrListIterator::logSource() const {
  DCHECK(valid());
  return logEntries_.at(idx_).get_cluster();
}

folly::StringPiece LogStrListIterator::logMsg() const {
  DCHECK(valid());
  return logEntries_.at(idx_).get_log_str();
}

}  // namespace nebula
