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
        wal::BufferFlusher* flusher,
        std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
        std::shared_ptr<thread::GenericThreadPool> workers,
        std::function<void(size_t idx, const char*, TermID)>
            leadershipLostCB,
        std::function<void(size_t idx, const char*, TermID)>
            becomeLeaderCB);

    LogID lastCommittedLogId() override {
        return 0;
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
    bool getLogMsg(LogID id, folly::StringPiece& msg) const;

public:
    int32_t commitTimes_ = 0;
    int32_t currLogId_ = -1;

private:
    const size_t idx_;
    std::shared_ptr<RaftexService> service_;

    std::unordered_map<LogID, std::string> data_;

    std::function<void(size_t idx, const char*, TermID)>
        leadershipLostCB_;
    std::function<void(size_t idx, const char*, TermID)>
        becomeLeaderCB_;
};

}  // namespace test
}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_TEST_TESTSHARD_H_

