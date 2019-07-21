/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/raftex/test/TestShard.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/BufferFlusher.h"

namespace nebula {
namespace raftex {
namespace test {

std::string encodeLearner(const HostAddr& addr) {
    std::string str;
    CommandType type = CommandType::ADD_LEARNER;
    str.append(reinterpret_cast<const char*>(&type), 1);
    str.append(reinterpret_cast<const char*>(&addr), sizeof(HostAddr));
    return str;
}

HostAddr decodeLearner(const folly::StringPiece& log) {
    HostAddr learner;
    memcpy(&learner.first, log.begin() + 1, sizeof(learner.first));
    memcpy(&learner.second, log.begin() + 1 + sizeof(learner.first), sizeof(learner.second));
    return learner;
}


TestShard::TestShard(size_t idx,
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
                        becomeLeaderCB)
        : RaftPart(1,   // clusterId
                   1,   // spaceId
                   partId,
                   addr,
                   walRoot,
                   flusher,
                   ioPool,
                   workers)
        , idx_(idx)
        , service_(svc)
        , leadershipLostCB_(leadershipLostCB)
        , becomeLeaderCB_(becomeLeaderCB) {
}


void TestShard::onLostLeadership(TermID term) {
    if (leadershipLostCB_) {
        leadershipLostCB_(idx_, idStr(), term);
    }
}


void TestShard::onElected(TermID term) {
    if (becomeLeaderCB_) {
        becomeLeaderCB_(idx_, idStr(), term);
    }
}


std::string TestShard::compareAndSet(const std::string& log) {
    switch (log[0]) {
        case 'T':
            return log.substr(1);
        default:
            return std::string();
    }
}


bool TestShard::commitLogs(std::unique_ptr<LogIterator> iter) {
    VLOG(2) << "TestShard: Committing logs";
    LogID firstId = -1;
    LogID lastId = -1;
    int32_t commitLogsNum = 0;
    while (iter->valid()) {
        if (firstId < 0) {
            firstId = iter->logId();
        }
        lastId = iter->logId();
        auto log = iter->logMsg();
        if (!log.empty()) {
            switch (static_cast<CommandType>(log[0])) {
                case CommandType::ADD_LEARNER: {
                    break;
                }
                default: {
                    VLOG(1) << idStr_ << "Write " << iter->logId() << ":" << log;
                    folly::RWSpinLock::WriteHolder wh(&lock_);
                    data_.emplace(iter->logId(), log.toString());
                    currLogId_ = iter->logId();
                    break;
                }
            }
            commitLogsNum++;
        }
        ++(*iter);
    }
    VLOG(2) << "TestShard: Committed log " << firstId << " to " << lastId;
    if (commitLogsNum > 0) {
        commitTimes_++;
    }
    return true;
}


size_t TestShard::getNumLogs() const {
    return data_.size();
}


bool TestShard::getLogMsg(LogID id, folly::StringPiece& msg) const {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = data_.find(id);
    if (it == data_.end()) {
        // Not found
        return false;
    }

    msg = it->second;
    return true;
}

}  // namespace test
}  // namespace raftex
}  // namespace nebula

