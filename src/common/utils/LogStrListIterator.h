/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_UTILS_LOGSTRLISTITERATOR_H_
#define COMMON_UTILS_LOGSTRLISTITERATOR_H_

#include "common/base/Base.h"
#include "common/utils/LogIterator.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {

class LogStrListIterator final : public LogIterator {
 public:
  LogStrListIterator(LogID firstLogId, TermID term, std::vector<cpp2::LogEntry> logEntries);

  LogIterator& operator++() override;

  bool valid() const override;

  LogID logId() const override;
  TermID logTerm() const override;
  ClusterID logSource() const override;
  folly::StringPiece logMsg() const override;

 private:
  const LogID firstLogId_;
  const TermID term_;
  size_t idx_;
  std::vector<cpp2::LogEntry> logEntries_;
};

}  // namespace nebula

#endif  // COMMON_UTILS_LOGSTRLISTITERATOR_H_
