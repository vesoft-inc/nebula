/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Part.h"
#include "kvstore/LogEncoder.h"
#include "utils/NebulaKeyUtils.h"
#include "kvstore/RocksEngineConfig.h"

DEFINE_int32(cluster_id, 0, "A unique id for each cluster");

namespace nebula {
namespace kvstore {

using nebula::raftex::AppendLogResult;

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
        LOG(INFO) << idStr_ << "Cannot fetch the last committed log id from the storage engine";
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
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(this->toResultCode(res));
        });
}


void Part::asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb) {
    std::string log = encodeMultiValues(OP_MULTI_PUT, keyValues);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(this->toResultCode(res));
        });
}


void Part::asyncRemove(folly::StringPiece key, KVCallback cb) {
    std::string log = encodeSingleValue(OP_REMOVE, key);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(this->toResultCode(res));
        });
}


void Part::asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb) {
    std::string log = encodeMultiValues(OP_MULTI_REMOVE, keys);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(this->toResultCode(res));
        });
}


void Part::asyncRemoveRange(folly::StringPiece start,
                            folly::StringPiece end,
                            KVCallback cb) {
    std::string log = encodeMultiValues(OP_REMOVE_RANGE, start, end);

    appendAsync(FLAGS_cluster_id, std::move(log))
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
            callback(this->toResultCode(res));
        });
}

void Part::sync(KVCallback cb) {
    sendCommandAsync("")
        .thenValue([this, callback = std::move(cb)] (AppendLogResult res) mutable {
        callback(this->toResultCode(res));
    });
}

void Part::asyncAtomicOp(raftex::AtomicOp op, KVCallback cb) {
    atomicOpAsync(std::move(op)).thenValue(
            [this, callback = std::move(cb)] (AppendLogResult res) mutable {
        callback(this->toResultCode(res));
    });
}

void Part::asyncAddLearner(const HostAddr& learner, KVCallback cb) {
    std::string log = encodeHost(OP_ADD_LEARNER, learner);
    sendCommandAsync(std::move(log))
        .thenValue([callback = std::move(cb), learner, this] (AppendLogResult res) mutable {
        LOG(INFO) << idStr_ << "add learner " << learner
                  << ", result: " << static_cast<int32_t>(this->toResultCode(res));
        callback(this->toResultCode(res));
    });
}

void Part::asyncTransferLeader(const HostAddr& target, KVCallback cb) {
    std::string log = encodeHost(OP_TRANS_LEADER, target);
    sendCommandAsync(std::move(log))
        .thenValue([callback = std::move(cb), target, this] (AppendLogResult res) mutable {
        LOG(INFO) << idStr_ << "transfer leader to " << target
                  << ", result: " << static_cast<int32_t>(this->toResultCode(res));
        callback(this->toResultCode(res));
    });
}

void Part::asyncAddPeer(const HostAddr& peer, KVCallback cb) {
    std::string log = encodeHost(OP_ADD_PEER, peer);
    sendCommandAsync(std::move(log))
        .thenValue([callback = std::move(cb), peer, this] (AppendLogResult res) mutable {
        LOG(INFO) << idStr_ << "add peer " << peer
                  << ", result: " << static_cast<int32_t>(this->toResultCode(res));
        callback(this->toResultCode(res));
    });
}

void Part::asyncRemovePeer(const HostAddr& peer, KVCallback cb) {
    std::string log = encodeHost(OP_REMOVE_PEER, peer);
    sendCommandAsync(std::move(log))
        .thenValue([callback = std::move(cb), peer, this] (AppendLogResult res) mutable {
        LOG(INFO) << idStr_ << "remove peer " << peer
                  << ", result: " << static_cast<int32_t>(this->toResultCode(res));
        callback(this->toResultCode(res));
    });
}


void Part::setBlocking(bool sign) {
    blocking_ = sign;
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
        case OP_REMOVE_RANGE: {
            auto range = decodeMultiValues(log);
            DCHECK_EQ(2, range.size());
            if (batch->removeRange(range[0], range[1]) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::removeRange()";
                return false;
            }
            break;
        }
        case OP_BATCH_WRITE: {
            auto data = decodeBatchValue(log);
            for (auto& op : data) {
                ResultCode code = ResultCode::SUCCEEDED;
                if (op.first == BatchLogType::OP_BATCH_PUT) {
                    code = batch->put(op.second.first, op.second.second);
                } else if (op.first == BatchLogType::OP_BATCH_REMOVE) {
                    code = batch->remove(op.second.first);
                } else if (op.first == BatchLogType::OP_BATCH_REMOVE_RANGE) {
                    code = batch->removeRange(op.second.first, op.second.second);
                }
                if (code != ResultCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch";
                    return false;
                }
            }
            break;
        }
        case OP_ADD_PEER:
        case OP_ADD_LEARNER: {
            break;
        }
        case OP_TRANS_LEADER: {
            auto newLeader = decodeHost(OP_TRANS_LEADER, log);
            auto ts = getTimestamp(log);
            if (ts > startTimeMs_) {
                commitTransLeader(newLeader);
            } else {
                LOG(INFO) << idStr_ << "Skip commit stale transfer leader " << newLeader
                          << ", the part is opened at " << startTimeMs_
                          << ", but the log timestamp is " << ts;
            }
            break;
        }
        case OP_REMOVE_PEER: {
            auto peer = decodeHost(OP_REMOVE_PEER, log);
            auto ts = getTimestamp(log);
            if (ts > startTimeMs_) {
                commitRemovePeer(peer);
            } else {
                LOG(INFO) << idStr_ << "Skip commit stale remove peer " << peer
                          << ", the part is opened at " << startTimeMs_
                          << ", but the log timestamp is " << ts;
            }
            break;
        }
        default: {
            LOG(WARNING) << idStr_ << "Unknown operation: " << static_cast<int32_t>(log[0]);
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
    return engine_->commitBatchWrite(std::move(batch),
                                     FLAGS_rocksdb_disable_wal,
                                     FLAGS_rocksdb_wal_sync) == ResultCode::SUCCEEDED;
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
    // For snapshot, we open the rocksdb's wal to avoid loss data if crash.
    if (ResultCode::SUCCEEDED != engine_->commitBatchWrite(std::move(batch), false)) {
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
                auto learner = decodeHost(OP_ADD_LEARNER, log);
                auto ts = getTimestamp(log);
                if (ts > startTimeMs_) {
                    LOG(INFO) << idStr_ << "preprocess add learner " << learner;
                    addLearner(learner);
                } else {
                    LOG(INFO) << idStr_ << "Skip stale add learner " << learner
                              << ", the part is opened at " << startTimeMs_
                              << ", but the log timestamp is " << ts;
                }
                break;
            }
            case OP_TRANS_LEADER: {
                auto newLeader = decodeHost(OP_TRANS_LEADER, log);
                auto ts = getTimestamp(log);
                if (ts > startTimeMs_) {
                    LOG(INFO) << idStr_ << "preprocess trans leader " << newLeader;
                    preProcessTransLeader(newLeader);
                } else {
                    LOG(INFO) << idStr_ << "Skip stale transfer leader " << newLeader
                              << ", the part is opened at " << startTimeMs_
                              << ", but the log timestamp is " << ts;
                }
                break;
            }
            case OP_ADD_PEER: {
                auto peer = decodeHost(OP_ADD_PEER, log);
                auto ts = getTimestamp(log);
                if (ts > startTimeMs_) {
                    LOG(INFO) << idStr_ << "preprocess add peer " << peer;
                    addPeer(peer);
                } else {
                    LOG(INFO) << idStr_ << "Skip stale add peer " << peer
                              << ", the part is opened at " << startTimeMs_
                              << ", but the log timestamp is " << ts;
                }
                break;
            }
            case OP_REMOVE_PEER: {
                auto peer = decodeHost(OP_REMOVE_PEER, log);
                auto ts = getTimestamp(log);
                if (ts > startTimeMs_) {
                    LOG(INFO) << idStr_ << "preprocess remove peer " << peer;
                    preProcessRemovePeer(peer);
                } else {
                    LOG(INFO) << idStr_ << "Skip stale remove peer " << peer
                              << ", the part is opened at " << startTimeMs_
                              << ", but the log timestamp is " << ts;
                }
                break;
            }
            default: {
                break;
            }
        }
    }
    return true;
}

ResultCode Part::toResultCode(raftex::AppendLogResult res) {
    switch (res) {
        case raftex::AppendLogResult::SUCCEEDED:
            return ResultCode::SUCCEEDED;
        case raftex::AppendLogResult::E_NOT_A_LEADER:
            return ResultCode::ERR_LEADER_CHANGED;
        case raftex::AppendLogResult::E_WRITE_BLOCKING:
            return ResultCode::ERR_WRITE_BLOCK_ERROR;
        case raftex::AppendLogResult::E_ATOMIC_OP_FAILURE:
            return ResultCode::ERR_ATOMIC_OP_FAILED;
        case raftex::AppendLogResult::E_BUFFER_OVERFLOW:
            PLOG_EVERY_N(ERROR, 100) << idStr_ << "RaftPart buffer is full";
            return ResultCode::ERR_CONSENSUS_ERROR;
        default:
            LOG(ERROR) << idStr_ << "Consensus error "
                       << static_cast<int32_t>(res);
            return ResultCode::ERR_CONSENSUS_ERROR;
    }
}



}  // namespace kvstore
}  // namespace nebula

