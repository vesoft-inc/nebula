/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Part.h"
#include "kvstore/LogEncoder.h"
#include "base/NebulaKeyUtils.h"

DEFINE_int32(cluster_id, 0, "A unique id for each cluster");

namespace nebula {
namespace kvstore {

using raftex::AppendLogResult;

namespace {

ResultCode toResultCode(AppendLogResult res) {
    switch (res) {
        case AppendLogResult::SUCCEEDED:
            return ResultCode::SUCCEEDED;
        case AppendLogResult::E_NOT_A_LEADER:
            return ResultCode::ERR_LEADER_CHANGED;
        default:
            return ResultCode::ERR_CONSENSUS_ERROR;
    }
}

}  // Anonymous namespace


Part::Part(GraphSpaceID spaceId,
           PartitionID partId,
           HostAddr localAddr,
           const std::string& walPath,
           KVEngine* engine,
           std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
           std::shared_ptr<thread::GenericThreadPool> workers,
           std::shared_ptr<folly::Executor> handlers,
           std::shared_ptr<raftex::SnapshotManager> snapshotMan)
        : RaftPart(FLAGS_cluster_id,
                   spaceId,
                   partId,
                   localAddr,
                   walPath,
                   ioPool,
                   workers,
                   handlers,
                   snapshotMan)
        , spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {
}


std::pair<LogID, TermID> Part::lastCommittedLogId() {
    std::string val;
    ResultCode res = engine_->get(NebulaKeyUtils::systemCommitKey(partId_), &val);
    if (res != ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Cannot fetch the last committed log id from the storage engine";
        return std::make_pair(0, 0);
    }
    CHECK_EQ(val.size(), sizeof(LogID) + sizeof(TermID));

    LogID lastId;
    memcpy(reinterpret_cast<void*>(&lastId), val.data(), sizeof(LogID));
    TermID termId;
    memcpy(reinterpret_cast<void*>(&termId), val.data() + sizeof(LogID), sizeof(TermID));

    return std::make_pair(lastId, termId);
}


void Part::asyncPut(folly::StringPiece key, folly::StringPiece value, KVCallback cb) {
    std::string log = encodeMultiValues(OP_PUT, key, value);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}


void Part::asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb) {
    std::string log = encodeMultiValues(OP_MULTI_PUT, keyValues);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}


void Part::asyncRemove(folly::StringPiece key, KVCallback cb) {
    std::string log = encodeSingleValue(OP_REMOVE, key);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}


void Part::asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb) {
    std::string log = encodeMultiValues(OP_MULTI_REMOVE, keys);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}


void Part::asyncRemovePrefix(folly::StringPiece prefix, KVCallback cb) {
    std::string log = encodeSingleValue(OP_REMOVE_PREFIX, prefix);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}


void Part::asyncRemoveRange(folly::StringPiece start,
                            folly::StringPiece end,
                            KVCallback cb) {
    std::string log = encodeMultiValues(OP_REMOVE_RANGE, start, end);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(toResultCode(res));
        });
}

void Part::asyncAtomicOp(raftex::AtomicOp op, KVCallback cb) {
    atomicOpAsync(std::move(op)).then([callback = std::move(cb)] (AppendLogResult res) mutable {
        callback(toResultCode(res));
    });
}

void Part::asyncAddLearner(const HostAddr& learner, KVCallback cb) {
    std::string log = encodeLearner(learner);
    sendCommandAsync(std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
        callback(toResultCode(res));
    });
}

void Part::asyncTransferLeader(const HostAddr& target, KVCallback cb) {
    std::string log = encodeTransLeader(target);
    sendCommandAsync(std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) mutable {
        callback(toResultCode(res));
    });
}

void Part::onLostLeadership(TermID term) {
    VLOG(1) << "Lost the leadership for the term " << term;
}


void Part::onElected(TermID term) {
    VLOG(1) << "Being elected as the leader for the term " << term;
}

void Part::onDiscoverNewLeader(HostAddr nLeader) {
    LOG(INFO) << idStr_ << "Find the new leader " << nLeader;
    if (newLeaderCb_) {
        newLeaderCb_(nLeader);
    }
}

bool Part::commitLogs(std::unique_ptr<LogIterator> iter) {
    auto batch = engine_->startBatchWrite();
    LogID lastId = -1;
    TermID lastTerm = -1;
    while (iter->valid()) {
        lastId = iter->logId();
        lastTerm = iter->logTerm();
        auto log = iter->logMsg();
        if (log.empty()) {
            VLOG(3) << idStr_ << "Skip the heartbeat!";
            ++(*iter);
            continue;
        }
        DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
        // Skip the timestamp (type of int64_t)
        switch (log[sizeof(int64_t)]) {
        case OP_PUT: {
            auto pieces = decodeMultiValues(log);
            DCHECK_EQ(2, pieces.size());
            if (batch->put(pieces[0], pieces[1]) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::put()";
                return false;
            }
            break;
        }
        case OP_MULTI_PUT: {
            auto kvs = decodeMultiValues(log);
            // Make the number of values are an even number
            DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
            for (size_t i = 0; i < kvs.size(); i += 2) {
                if (batch->put(kvs[i], kvs[i + 1]) != ResultCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch::put()";
                    return false;
                }
            }
            break;
        }
        case OP_REMOVE: {
            auto key = decodeSingleValue(log);
            if (batch->remove(key) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::remove()";
                return false;
            }
            break;
        }
        case OP_MULTI_REMOVE: {
            auto keys = decodeMultiValues(log);
            for (auto k : keys) {
                if (batch->remove(k) != ResultCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch::remove()";
                    return false;
                }
            }
            break;
        }
        case OP_REMOVE_PREFIX: {
            auto prefix = decodeSingleValue(log);
            if (batch->removePrefix(prefix) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::removePrefix()";
                return false;
            }
            break;
        }
        case OP_REMOVE_RANGE: {
            auto range = decodeMultiValues(log);
            DCHECK_EQ(2, range.size());
            if (batch->removeRange(range[0], range[1]) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::removeRange()";
                return false;
            }
            break;
        }
        case OP_ADD_LEARNER: {
            break;
        }
        case OP_TRANS_LEADER: {
            auto newLeader = decodeTransLeader(log);
            commitTransLeader(newLeader);
            LOG(INFO) << idStr_ << "Transfer leader to " << newLeader;
            break;
        }
        default: {
            LOG(FATAL) << idStr_ << "Unknown operation: " << static_cast<uint8_t>(log[0]);
        }
        }

        ++(*iter);
    }

    if (lastId >= 0) {
        if (putCommitMsg(batch.get(), lastId, lastTerm) != ResultCode::SUCCEEDED) {
            LOG(ERROR) << idStr_ << "Commit msg failed";
            return false;
        }
    }
    return engine_->commitBatchWrite(std::move(batch)) == ResultCode::SUCCEEDED;
}

std::pair<int64_t, int64_t> Part::commitSnapshot(const std::vector<std::string>& rows,
                                                 LogID committedLogId,
                                                 TermID committedLogTerm,
                                                 bool finished) {
    auto batch = engine_->startBatchWrite();
    int64_t count = 0;
    int64_t size = 0;
    for (auto& row : rows) {
        count++;
        size += row.size();
        auto kv = decodeKV(row);
        if (ResultCode::SUCCEEDED != batch->put(kv.first, kv.second)) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return std::make_pair(0, 0);
        }
    }
    if (finished) {
        if (ResultCode::SUCCEEDED != putCommitMsg(batch.get(), committedLogId, committedLogTerm)) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return std::make_pair(0, 0);
        }
    }
    if (ResultCode::SUCCEEDED != engine_->commitBatchWrite(std::move(batch))) {
        LOG(ERROR) << idStr_ << "Put failed in commit";
        return std::make_pair(0, 0);
    }
    return std::make_pair(count, size);
}

ResultCode Part::putCommitMsg(WriteBatch* batch, LogID committedLogId, TermID committedLogTerm) {
    std::string commitMsg;
    commitMsg.reserve(sizeof(LogID) + sizeof(TermID));
    commitMsg.append(reinterpret_cast<char*>(&committedLogId), sizeof(LogID));
    commitMsg.append(reinterpret_cast<char*>(&committedLogTerm), sizeof(TermID));
    return batch->put(NebulaKeyUtils::systemCommitKey(partId_), commitMsg);
}

bool Part::preProcessLog(LogID logId,
                         TermID termId,
                         ClusterID clusterId,
                         const std::string& log) {
    VLOG(3) << idStr_ << "logId " << logId
            << ", termId " << termId
            << ", clusterId " << clusterId;
    if (!log.empty()) {
        switch (log[sizeof(int64_t)]) {
            case OP_ADD_LEARNER: {
                auto learner = decodeLearner(log);
                addLearner(learner);
                LOG(INFO) << idStr_ << "Preprocess add learner " << learner;
                break;
            }
            case OP_TRANS_LEADER: {
                auto newLeader = decodeTransLeader(log);
                preProcessTransLeader(newLeader);
                LOG(INFO) << idStr_ << "Preprocess transfer leader to " << newLeader;
                break;
            }
            default: {
                break;
            }
        }
    }
    return true;
}

}  // namespace kvstore
}  // namespace nebula

