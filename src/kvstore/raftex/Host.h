/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_HOST_H_
#define RAFTEX_HOST_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include "interface/gen-cpp2/raftex_types.h"
#include "gen-cpp2/RaftexServiceAsyncClient.h"
#include "thrift/ThriftClientManager.h"

namespace folly {
class EventBase;
}  // namespace folly


namespace nebula {
namespace raftex {

class RaftPart;

class Host final : public std::enable_shared_from_this<Host> {
public:
    Host(const HostAddr& addr, std::shared_ptr<RaftPart> part, bool isLearner = false);

    ~Host() {
        LOG(INFO) << idStr_ << " The host has been destroyed!";
    }

    const char* idStr() const {
        return idStr_.c_str();
    }

    // This will be called when the shard lost its leadership
    void pause() {
        std::lock_guard<std::mutex> g(lock_);
        paused_ = true;
    }

    // This will be called when the shard becomes the leader
    void resume() {
        std::lock_guard<std::mutex> g(lock_);
        paused_ = false;
    }

    void stop() {
        std::lock_guard<std::mutex> g(lock_);
        stopped_ = true;
    }

    void waitForStop();

    bool isLearner() const {
        return isLearner_;
    }

    folly::Future<cpp2::AskForVoteResponse> askForVote(
        const cpp2::AskForVoteRequest& req);

    // When logId == lastLogIdSent, it is a heartbeat
    folly::Future<cpp2::AppendLogResponse> appendLogs(
        folly::EventBase* eb,
        TermID term,                // Current term
        LogID logId,                // The last log to be sent
        LogID committedLogId,       // The last committed log id
        TermID lastLogTermSent,     // The last log term being sent
        LogID lastLogIdSent);       // The last log id being sent

    const HostAddr& address() const {
        return addr_;
    }

private:
    cpp2::ErrorCode checkStatus() const;

    folly::Future<cpp2::AppendLogResponse> sendAppendLogRequest(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req);

    void appendLogsInternal(
        folly::EventBase* eb,
        std::shared_ptr<cpp2::AppendLogRequest> req);

    std::shared_ptr<cpp2::AppendLogRequest> prepareAppendLogRequest() const;

    bool noRequest() const;

    void setResponse(const cpp2::AppendLogResponse& r);

    thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>& tcManager() {
        static thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient> manager;
        return manager;
    }

private:
    // <term, logId, committedLogId, lastLogTermSent, lastLogIdSent>
    using Request = std::tuple<TermID, LogID, LogID, TermID, LogID>;

    std::shared_ptr<RaftPart> part_;
    const HostAddr addr_;
    bool isLearner_ = false;
    const std::string idStr_;

    mutable std::mutex lock_;

    bool paused_{false};
    bool stopped_{false};

    bool requestOnGoing_{false};
    std::condition_variable noMoreRequestCV_;
    folly::SharedPromise<cpp2::AppendLogResponse> promise_;
    folly::SharedPromise<cpp2::AppendLogResponse> cachingPromise_;

    Request pendingReq_{0, 0, 0, 0, 0};

    // These logId and term pointing to the latest log we need to send
    LogID logIdToSend_{0};
    TermID logTermToSend_{0};

    // The previous log before this batch
    LogID lastLogIdSent_{0};
    TermID lastLogTermSent_{0};

    LogID committedLogId_{0};
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_HOST_H_

