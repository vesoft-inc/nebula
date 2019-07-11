/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/Part.h"
#include "kvstore/LogEncoder.h"

DEFINE_int32(cluster_id, 0, "A unique id for each cluster");

namespace nebula {
namespace kvstore {

using raftex::AppendLogResult;

const char kLastCommittedIdKey[] = "_last_committed_log_id";

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
           wal::BufferFlusher* flusher)
        : RaftPart(FLAGS_cluster_id,
                   spaceId,
                   partId,
                   localAddr,
                   walPath,
                   flusher,
                   ioPool,
                   workers)
        , spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {
}


LogID Part::lastCommittedLogId() {
    std::string val;
    ResultCode res = engine_->get(kLastCommittedIdKey, &val);
    if (res != ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Cannot fetch the last committed log id from the storage engine";
        return 0;
    }
    CHECK_EQ(val.size(), sizeof(LogID));

    LogID lastId;
    memcpy(reinterpret_cast<void*>(&lastId), val.data(), sizeof(LogID));

    return lastId;
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


void Part::onLostLeadership(TermID term) {
    VLOG(1) << "Lost the leadership for the term " << term;
}


void Part::onElected(TermID term) {
    VLOG(1) << "Being elected as the leader for the term " << term;
}


std::string Part::compareAndSet(const std::string& log) {
    UNUSED(log);
    LOG(FATAL) << "To be implemented";
}


bool Part::commitLogs(std::unique_ptr<LogIterator> iter) {
    auto batch = engine_->startBatchWrite();
    LogID lastId = -1;
    while (iter->valid()) {
        lastId = iter->logId();
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
                LOG(ERROR) << "Failed to call WriteBatch::put()";
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
                    LOG(ERROR) << "Failed to call WriteBatch::put()";
                    return false;
                }
            }
            break;
        }
        case OP_REMOVE: {
            auto key = decodeSingleValue(log);
            if (batch->remove(key) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::remove()";
                return false;
            }
            break;
        }
        case OP_MULTI_REMOVE: {
            auto keys = decodeMultiValues(log);
            for (auto k : keys) {
                if (batch->remove(k) != ResultCode::SUCCEEDED) {
                    LOG(ERROR) << "Failed to call WriteBatch::remove()";
                    return false;
                }
            }
            break;
        }
        case OP_REMOVE_PREFIX: {
            auto prefix = decodeSingleValue(log);
            if (batch->removePrefix(prefix) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::removePrefix()";
                return false;
            }
            break;
        }
        case OP_REMOVE_RANGE: {
            auto range = decodeMultiValues(log);
            DCHECK_EQ(2, range.size());
            if (batch->removeRange(range[0], range[1]) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::removeRange()";
                return false;
            }
            break;
        }
        default: {
            LOG(FATAL) << "Unknown operation: " << static_cast<uint8_t>(log[0]);
        }
        }

        ++(*iter);
    }

    if (lastId >= 0) {
        batch->put(kLastCommittedIdKey,
                   folly::StringPiece(reinterpret_cast<char*>(&lastId), sizeof(LogID)));
    }

    return engine_->commitBatchWrite(std::move(batch)) == ResultCode::SUCCEEDED;
}

bool Part::preProcessLog(LogID logId,
                         TermID termId,
                         ClusterID clusterId,
                         const std::string& log) {
    VLOG(3) << idStr_ << "logId " << logId
            << ", termId " << termId
            << ", clusterId " << clusterId
            << ", log " << log;
    return true;
}

}  // namespace kvstore
}  // namespace nebula

