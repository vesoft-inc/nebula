/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_TEST_TESTSHARD_H_
#define RAFTEX_TEST_TESTSHARD_H_

#include "common/base/Base.h"
#include "kvstore/raftex/RaftPart.h"
#include "kvstore/raftex/RaftexService.h"

namespace nebula {
namespace raftex {

// class RaftexService;

namespace test {

enum class CommandType : int8_t {
    ADD_LEARNER = 0x01,
    TRANSFER_LEADER = 0x02,
    ADD_PEER = 0x03,
    REMOVE_PEER = 0x04,
};
std::string encodeLearner(const HostAddr& addr);

HostAddr decodeLearner(const folly::StringPiece& log);

folly::Optional<std::string> compareAndSet(const std::string& log);

std::string encodeTransferLeader(const HostAddr& addr);

HostAddr decodeTransferLeader(const folly::StringPiece& log);

std::string encodeSnapshotRow(LogID logId, const std::string& row);

std::pair<LogID, std::string> decodeSnapshotRow(const std::string& rawData);

std::string encodeAddPeer(const HostAddr& addr);

HostAddr decodeAddPeer(const folly::StringPiece& log);

std::string encodeRemovePeer(const HostAddr& addr);

HostAddr decodeRemovePeer(const folly::StringPiece& log);

class TestShard : public RaftPart {
    friend class SnapshotManagerImpl;
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
        std::shared_ptr<SnapshotManager> snapshotMan,
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
    void onDiscoverNewLeader(HostAddr) override {}

    nebula::cpp2::ErrorCode commitLogs(std::unique_ptr<LogIterator> iter, bool wait) override;

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
                case CommandType::TRANSFER_LEADER: {
                    auto nLeader = decodeTransferLeader(log);
                    preProcessTransLeader(nLeader);
                    LOG(INFO) << idStr_ << "Preprocess transleader " << nLeader;
                    break;
                }
                case CommandType::ADD_PEER: {
                    auto peer = decodeAddPeer(log);
                    addPeer(peer);
                    LOG(INFO) << idStr_ << "Add peer " << peer;
                    break;
                }
                case CommandType::REMOVE_PEER: {
                    auto peer = decodeRemovePeer(log);
                    preProcessRemovePeer(peer);
                    LOG(INFO) << idStr_ << "Remove peer " << peer;
                    break;
                }
                default: {
                    break;
                }
            }
        }
        return true;
    }

    std::pair<int64_t, int64_t> commitSnapshot(const std::vector<std::string>& data,
                                               LogID committedLogId,
                                               TermID committedLogTerm,
                                               bool finished) override;

    void cleanup() override;

    size_t getNumLogs() const;
    bool getLogMsg(size_t index, folly::StringPiece& msg);

public:
    int32_t commitTimes_ = 0;
    int32_t currLogId_ = -1;

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

class SnapshotManagerImpl : public SnapshotManager {
public:
    explicit SnapshotManagerImpl(RaftexService* service)
        : service_(service) {
        CHECK_NOTNULL(service);
    }

    ~SnapshotManagerImpl() {
        LOG(INFO) << "~SnapshotManagerImpl()";
    }

    void accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 SnapshotCallback cb) override {
        auto part = std::dynamic_pointer_cast<TestShard>(service_->findPart(spaceId, partId));
        CHECK(!!part);
        int64_t totalCount = 0;
        int64_t totalSize = 0;
        std::vector<std::string> data;
        folly::RWSpinLock::ReadHolder rh(&part->lock_);
        for (auto& row : part->data_) {
            if (data.size() > 100) {
                LOG(INFO) << part->idStr_ << "Send snapshot total rows " << data.size()
                          << ", total count sended " << totalCount
                          << ", total size sended " << totalSize
                          << ", finished false";
                cb(data, totalCount, totalSize, SnapshotStatus::IN_PROGRESS);
                data.clear();
            }
            auto encoded = encodeSnapshotRow(row.first, row.second);
            totalSize += encoded.size();
            totalCount++;
            data.emplace_back(std::move(encoded));
        }
        LOG(INFO) << part->idStr_ << "Send snapshot total rows " << data.size()
                  << ", total count sended " << totalCount
                  << ", total size sended " << totalSize
                  << ", finished true";
        cb(data, totalCount, totalSize, SnapshotStatus::DONE);
    }

    RaftexService* service_;
};

}  // namespace test
}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_TEST_TESTSHARD_H_

