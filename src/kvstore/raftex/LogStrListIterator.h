/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_LOGSTRLISTITERATOR_H_
#define RAFTEX_LOGSTRLISTITERATOR_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/raftex_types.h"
#include "utils/LogIterator.h"

namespace nebula {
namespace raftex {

class LogStrListIterator final : public LogIterator {
public:
    LogStrListIterator(LogID firstLogId,
                       TermID term,
                       std::vector<cpp2::LogEntry> logEntries);

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

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_LOGSTRLISTITERATOR_H_

