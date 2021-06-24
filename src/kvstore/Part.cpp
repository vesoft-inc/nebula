/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Part.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RocksEngineConfig.h"
#include "utils/NebulaKeyUtils.h"

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
           std::shared_ptr<raftex::SnapshotManager> snapshotMan,
           std::shared_ptr<RaftClient> clientMan,
           std::shared_ptr<DiskManager> diskMan)
        : RaftPart(FLAGS_cluster_id,
                   spaceId,
                   partId,
                   localAddr,
                   walPath,
                   ioPool,
                   workers,
                   handlers,
                   snapshotMan,
                   clientMan,
                   diskMan)
        , spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {
}


std::pair<LogID, TermID> Part::lastCommittedLogId() {
    std::string val;
    auto res = engine_->get(NebulaKeyUtils::systemCommitKey(partId_), &val);
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
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

void Part::asyncAppendBatch(std::string&& batch, KVCallback cb) {
    appendAsync(FLAGS_cluster_id, std::move(batch))
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

cpp2::ErrorCode Part::commitLogs(std::unique_ptr<LogIterator> iter, bool wait) {
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
            auto code = batch->put(pieces[0], pieces[1]);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::put()";
                return code;
            }
            break;
        }
        case OP_MULTI_PUT: {
            auto kvs = decodeMultiValues(log);
            // Make the number of values are an even number
            DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
            for (size_t i = 0; i < kvs.size(); i += 2) {
                auto code = batch->put(kvs[i], kvs[i + 1]);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch::put()";
                    return code;
                }
            }
            break;
        }
        case OP_REMOVE: {
            auto key = decodeSingleValue(log);
            auto code = batch->remove(key);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::remove()";
                return code;
            }
            break;
        }
        case OP_MULTI_REMOVE: {
            auto keys = decodeMultiValues(log);
            for (auto k : keys) {
                auto code = batch->remove(k);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch::remove()";
                    return code;
                }
            }
            break;
        }
        case OP_REMOVE_RANGE: {
            auto range = decodeMultiValues(log);
            DCHECK_EQ(2, range.size());
            auto code = batch->removeRange(range[0], range[1]);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << idStr_ << "Failed to call WriteBatch::removeRange()";
                return code;
            }
            break;
        }
        case OP_BATCH_WRITE: {
            auto data = decodeBatchValue(log);
            for (auto& op : data) {
                auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
                if (op.first == BatchLogType::OP_BATCH_PUT) {
                    code = batch->put(op.second.first, op.second.second);
                } else if (op.first == BatchLogType::OP_BATCH_REMOVE) {
                    code = batch->remove(op.second.first);
                } else if (op.first == BatchLogType::OP_BATCH_REMOVE_RANGE) {
                    code = batch->removeRange(op.second.first, op.second.second);
                }
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(ERROR) << idStr_ << "Failed to call WriteBatch";
                    return code;
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
        auto code = putCommitMsg(batch.get(), lastId, lastTerm);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << idStr_ << "Commit msg failed";
            return code;
        }
    }
    return engine_->commitBatchWrite(
        std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, wait);
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
        if (nebula::cpp2::ErrorCode::SUCCEEDED != batch->put(kv.first, kv.second)) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return std::make_pair(0, 0);
        }
    }
    if (finished) {
        auto retCode = putCommitMsg(batch.get(), committedLogId, committedLogTerm);
        if (nebula::cpp2::ErrorCode::SUCCEEDED != retCode) {
            LOG(ERROR) << idStr_ << "Put failed in commit";
            return std::make_pair(0, 0);
        }
    }
    // For snapshot, we open the rocksdb's wal to avoid loss data if crash.
    auto code = engine_->commitBatchWrite(
        std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << idStr_ << "Put failed in commit";
        return std::make_pair(0, 0);
    }
    return std::make_pair(count, size);
}

nebula::cpp2::ErrorCode
Part::putCommitMsg(WriteBatch* batch, LogID committedLogId, TermID committedLogTerm) {
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

void Part::cleanup() {
    LOG(INFO) << idStr_ << "Clean rocksdb commit key";
    auto res = engine_->remove(NebulaKeyUtils::systemCommitKey(partId_));
    if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(WARNING) << idStr_ << "Remove the committedLogId failed, error "
                     << static_cast<int32_t>(res);
    }
    return;
}

// TODO(pandasheep) unify raft errorcode
nebula::cpp2::ErrorCode Part::toResultCode(raftex::AppendLogResult res) {
    switch (res) {
        case raftex::AppendLogResult::SUCCEEDED:
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        case raftex::AppendLogResult::E_NOT_A_LEADER:
            return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
        case raftex::AppendLogResult::E_WRITE_BLOCKING:
            return nebula::cpp2::ErrorCode::E_CHECKPOINT_BLOCKED;
        case raftex::AppendLogResult::E_ATOMIC_OP_FAILURE:
            return nebula::cpp2::ErrorCode::E_ATOMIC_OP_FAILED;
        case raftex::AppendLogResult::E_BUFFER_OVERFLOW:
            return nebula::cpp2::ErrorCode::E_CONSENSUS_ERROR;
        default:
            LOG(ERROR) << idStr_ << "Consensus error "
                       << static_cast<int32_t>(res);
            return nebula::cpp2::ErrorCode::E_CONSENSUS_ERROR;
    }
}

}  // namespace kvstore
}  // namespace nebula
