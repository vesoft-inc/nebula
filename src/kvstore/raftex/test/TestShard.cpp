/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "kvstore/raftex/test/TestShard.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/raftex/Host.h"

namespace nebula {
namespace raftex {
namespace test {

std::string serializeHostAddr(const HostAddr& host) {
    std::string ret;
    ret.reserve(sizeof(size_t) + 15 + sizeof(Port));    // 255.255.255.255
    size_t len = host.host.size();
    ret.append(reinterpret_cast<char*>(&len), sizeof(size_t))
       .append(host.host.data(), len)
       .append(reinterpret_cast<const char*>(&host.port), sizeof(Port));
    return ret;
}

HostAddr deserializeHostAddr(folly::StringPiece raw) {
    HostAddr addr;
    CHECK_GE(raw.size(), sizeof(size_t) + sizeof(Port));  // host may be ""
    size_t offset = 0;
    size_t len = *reinterpret_cast<const size_t*>(raw.begin() + offset);
    offset += sizeof(size_t);
    addr.host = std::string(raw.begin() + offset, len);
    offset += len;
    addr.port = *reinterpret_cast<const Port*>(raw.begin() + offset);
    return addr;
}

std::string encodeLearner(const HostAddr& addr) {
    std::string str;
    CommandType type = CommandType::ADD_LEARNER;
    str.append(reinterpret_cast<const char*>(&type), 1);
    str.append(serializeHostAddr(addr));
    return str;
}

HostAddr decodeLearner(const folly::StringPiece& log) {
    auto rawlog = log;
    rawlog.advance(1);
    return deserializeHostAddr(rawlog);
}

folly::Optional<std::string> compareAndSet(const std::string& log) {
    switch (log[0]) {
        case 'T':
            return log.substr(1);
        default:
            return folly::none;
    }
}

std::string encodeTransferLeader(const HostAddr& addr) {
    std::string str;
    CommandType type = CommandType::TRANSFER_LEADER;
    str.append(reinterpret_cast<const char*>(&type), 1);
    str.append(serializeHostAddr(addr));
    return str;
}

HostAddr decodeTransferLeader(const folly::StringPiece& log) {
    auto raw = log;
    raw.advance(1);
    return deserializeHostAddr(raw);
}

std::string encodeSnapshotRow(LogID logId, const std::string& row) {
    std::string rawData;
    rawData.reserve(sizeof(LogID) + row.size());
    rawData.append(reinterpret_cast<const char*>(&logId), sizeof(logId));
    rawData.append(row.data(), row.size());
    return rawData;
}

std::pair<LogID, std::string> decodeSnapshotRow(const std::string& rawData) {
    LogID id = *reinterpret_cast<const LogID*>(rawData.data());
    auto str = rawData.substr(sizeof(LogID));
    return std::make_pair(id, std::move(str));
}

std::string encodeAddPeer(const HostAddr& addr) {
    std::string str;
    CommandType type = CommandType::ADD_PEER;
    str.append(reinterpret_cast<const char*>(&type), 1);
    str.append(serializeHostAddr(addr));
    return str;
}

HostAddr decodeAddPeer(const folly::StringPiece& log) {
    auto rawlog = log;
    rawlog.advance(1);
    return deserializeHostAddr(rawlog);
}

std::string encodeRemovePeer(const HostAddr& addr) {
    std::string str;
    CommandType type = CommandType::REMOVE_PEER;
    str.append(reinterpret_cast<const char*>(&type), 1);
    str.append(serializeHostAddr(addr));
    return str;
}

HostAddr decodeRemovePeer(const folly::StringPiece& log) {
    auto rawlog = log;
    rawlog.advance(1);
    return deserializeHostAddr(rawlog);
}

std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>>
getClientMan() {
    static std::shared_ptr<thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>>
        clientMan(new thrift::ThriftClientManager<cpp2::RaftexServiceAsyncClient>());
    return clientMan;
}

TestShard::TestShard(size_t idx,
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
                        becomeLeaderCB)
        : RaftPart(1,   // clusterId
                   1,   // spaceId
                   partId,
                   addr,
                   walRoot,
                   ioPool,
                   workers,
                   handlersPool,
                   snapshotMan,
                   getClientMan(),
                   nullptr)
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


nebula::cpp2::ErrorCode TestShard::commitLogs(std::unique_ptr<LogIterator> iter, bool) {
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
                case CommandType::TRANSFER_LEADER: {
                    auto nLeader = decodeTransferLeader(log);
                    commitTransLeader(nLeader);
                    break;
                }
                case CommandType::REMOVE_PEER: {
                    auto peer = decodeRemovePeer(log);
                    commitRemovePeer(peer);
                    break;
                }
                case CommandType::ADD_PEER:
                case CommandType::ADD_LEARNER: {
                    break;
                }
                default: {
                    folly::RWSpinLock::WriteHolder wh(&lock_);
                    currLogId_ = iter->logId();
                    data_.emplace_back(currLogId_, log.toString());
                    VLOG(1) << idStr_ << "Write: " << log << ", LogId: " << currLogId_
                            << " state machine log size: " << data_.size();
                    break;
                }
            }
            commitLogsNum++;
        }
        ++(*iter);
    }
    VLOG(2) << "TestShard: " << idStr_ << "Committed log " << firstId << " to " << lastId;
    if (lastId > -1) {
        lastCommittedLogId_ = lastId;
    }
    if (commitLogsNum > 0) {
        commitTimes_++;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::pair<int64_t, int64_t> TestShard::commitSnapshot(const std::vector<std::string>& data,
                                                      LogID committedLogId,
                                                      TermID committedLogTerm,
                                                      bool finished) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    int64_t count = 0;
    int64_t size = 0;
    for (auto& row : data) {
        count++;
        size += row.size();
        auto idData = decodeSnapshotRow(row);
        VLOG(1) << idStr_ << "Commit row logId " << idData.first << ", log " << idData.second;
        data_.emplace_back(idData.first, std::move(idData.second));
    }
    if (finished) {
        lastCommittedLogId_ = committedLogId;
        LOG(INFO) << idStr_ << "Commit the snapshot committedLogId " << committedLogId
                  << ", term " << committedLogTerm;
    }
    return std::make_pair(count, size);
}

void TestShard::cleanup() {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    data_.clear();
    lastCommittedLogId_ = 0;
}

size_t TestShard::getNumLogs() const {
    return data_.size();
}

bool TestShard::getLogMsg(size_t index, folly::StringPiece& msg) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    if (index > data_.size()) {
        return false;
    }
    msg = data_[index].second;
    return true;
}

}  // namespace test
}  // namespace raftex
}  // namespace nebula

