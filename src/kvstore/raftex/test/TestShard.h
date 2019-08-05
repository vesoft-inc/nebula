/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_TEST_TESTSHARD_H_
#define RAFTEX_TEST_TESTSHARD_H_

#include "base/Base.h"
#include "kvstore/raftex/RaftPart.h"

namespace nebula {
namespace raftex {

class RaftexService;

namespace test {

enum class CommandType : int8_t {
    ADD_LEARNER = 0x01,
};

std::string encodeLearner(const HostAddr& addr);

HostAddr decodeLearner(const folly::StringPiece& log);

class TestShard : public RaftPart {
public:
    TestShard(
        size_t idx,
        std::shared_ptr<RaftexService> svc,
        PartitionID partId,
        HostAddr addr,
        const folly::StringPiece walRoot,
        std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
        std::shared_ptr<thread::GenericThreadPool> workers,
        std::shared_ptr<folly::Executor> handlersPool,
        std::function<void(size_t idx, const char*, TermID)>
            leadershipLostCB,
        std::function<void(size_t idx, const char*, TermID)>
            becomeLeaderCB);

    std::pair<LogID, TermID> lastCommittedLogId() override {
        return std::make_pair(committedLogId_, term_);
    }

    std::shared_ptr<RaftexService> getService() const {
        return service_;
    }

    size_t index() const {
        return idx_;
    }

    void onLostLeadership(TermID term) override;
    void onElected(TermID term) override;

    std::string compareAndSet(const std::string& log) override;
    bool commitLogs(std::unique_ptr<LogIterator> iter) override;

    bool preProcessLog(LogID,
                       TermID,
                       ClusterID,
                       const std::string& log) override {
        if (!log.empty()) {
            switch (static_cast<CommandType>(log[0])) {
                case CommandType::ADD_LEARNER: {
                    auto learner = decodeLearner(log);
                    addLearner(learner);
                    LOG(INFO) << idStr_ << "Add learner " << learner;
                    break;
                }
                default: {
                    break;
                }
            }
        }
        return true;
    }

    size_t getNumLogs() const;
    bool getLogMsg(size_t index, folly::StringPiece& msg);

public:
    int32_t commitTimes_ = 0;
    int32_t currLogId_ = -1;
    bool isRunning_ = false;

private:
    const size_t idx_;
    std::shared_ptr<RaftexService> service_;

    std::vector<std::pair<LogID, std::string>> data_;
    LogID lastCommittedLogId_ = 0L;
    mutable folly::RWSpinLock lock_;

    std::function<void(size_t idx, const char*, TermID)>
        leadershipLostCB_;
    std::function<void(size_t idx, const char*, TermID)>
        becomeLeaderCB_;
};

}  // namespace test
}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_TEST_TESTSHARD_H_

