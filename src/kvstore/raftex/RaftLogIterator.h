/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/Base.h"
#include "common/utils/LogIterator.h"
#include "interface/gen-cpp2/raftex_types.h"

namespace nebula {
namespace raftex {

class RaftLogIterator final : public LogIterator {
 public:
  RaftLogIterator(LogID firstLogId, std::vector<cpp2::RaftLogEntry> logEntries);

  RaftLogIterator& operator++() override;

  bool valid() const override;

  LogID logId() const override;

  TermID logTerm() const override;

  ClusterID logSource() const override;

  folly::StringPiece logMsg() const override;

 private:
  size_t idx_;
  const LogID firstLogId_;
  std::vector<cpp2::RaftLogEntry> logEntries_;
};

}  // namespace raftex
}  // namespace nebula
