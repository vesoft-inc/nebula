/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_HOST_H_
#define RAFTEX_HOST_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include "interface/gen-cpp2/raftex_types.h"

namespace folly {
class EventBase;
}  // namespace folly


namespace vesoft {
namespace raftex {

class RaftPart;

class Host final : public std::enable_shared_from_this<Host> {
public:
    Host(const HostAddr& addr, std::shared_ptr<RaftPart> part);

    void stop() {
        std::lock_guard<std::mutex> g(lock_);
        stopped_ = true;
    }

    void waitForStop();

    folly::Future<cpp2::AppendLogResponse> appendLogs(
        folly::EventBase* eb,
        TermID term,            // Current term
        LogID logId,            // The last log to be sent
        LogID committedLogId,   // The last committed log id
        TermID prevLogTerm,     // The previous log term
        LogID prevLogId);       // The previous log id


private:
    folly::Future<cpp2::AppendLogResponse> sendAppendLogRequest(
        std::shared_ptr<cpp2::AppendLogRequest> req);

    folly::Future<cpp2::AppendLogResponse> appendLogsInternal(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req);

    std::shared_ptr<cpp2::AppendLogRequest> prepareAppendLogRequest()
        const;


private:
    std::shared_ptr<RaftPart> part_;
    const HostAddr addr_;
    const std::string idStr_;

    mutable std::mutex lock_;

    bool stopped_{false};

    bool requestOnGoing_{false};
    std::condition_variable noMoreRequestCV_;
    folly::Promise<cpp2::AppendLogResponse> promise_;
    folly::Future<cpp2::AppendLogResponse> future_;

    // These logId and term pointing to the latest log we need to send
    LogID logIdToSend_{0};
    TermID logTermToSend_{0};

    // The previous log before this batch
    LogID prevLogId_{0};
    TermID prevLogTerm_{0};

    LogID committedLogId_{0};

    // The last log has been sent
    LogID lastLogIdSent_{0};
    TermID lastLogTermSent_{0};
};

}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_HOST_H_

