/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/Part.h"
#include "wal/BufferFlusher.h"

DEFINE_int32(cluster_id, 0, "A unique id for each cluster");

namespace nebula {
namespace kvstore {

namespace {

static const char OP_PUT            = 0x1;
static const char OP_MULTI_PUT      = 0x2;
static const char OP_REMOVE         = 0x3;
static const char OP_MULTI_REMOVE   = 0x4;
static const char OP_REMOVE_PREFIX  = 0x5;
static const char OP_REMOVE_RANGE   = 0x6;

wal::BufferFlusher* getBufferFlusher() {
    static wal::BufferFlusher flusher;
    return &flusher;
}

}  // Anonymous namespace


Part::Part(GraphSpaceID spaceId,
           PartitionID partId,
           HostAddr localAddr,
           const std::string& walPath,
           KVEngine* engine,
           std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
           std::shared_ptr<thread::GenericThreadPool> workers)
        : RaftPart(FLAGS_cluster_id,
                   spaceId,
                   partId,
                   localAddr,
                   walPath,
                   getBufferFlusher(),
                   ioPool,
                   workers)
        , spaceId_(spaceId)
        , partId_(partId)
        , walPath_(walPath)
        , engine_(engine) {
}


void Part::asyncPut(folly::StringPiece key, folly::StringPiece value, KVCallback cb) {
    std::string log;
    log.reserve(key.size() + value.size() + 9);
    log.append(&OP_PUT, 1);
    uint32_t len = static_cast<uint32_t>(key.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(key.begin(), key.size());
    len = static_cast<uint32_t>(value.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(value.begin(), value.size());

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
        });
}


void Part::asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb) {
    size_t totalLen = 0;
    for (auto& kv : keyValues) {
        totalLen += (2 * sizeof(uint32_t) + kv.first.size() + kv.second.size());
    }

    std::string log;
    log.reserve(totalLen + sizeof(uint32_t) + 1);

    log.append(&OP_MULTI_PUT, 1);
    uint32_t numKVs = keyValues.size();
    log.append(reinterpret_cast<char*>(&numKVs), sizeof(uint32_t));
    for (auto& kv : keyValues) {
        uint32_t len = kv.first.size();
        log.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        log.append(kv.first.data(), len);
        len = kv.second.size();
        log.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        log.append(kv.second.data(), len);
    }

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
        });
}


void Part::asyncRemove(folly::StringPiece key, KVCallback cb) {
    std::string log;
    log.reserve(key.size() + 5);
    log.append(&OP_REMOVE, 1);
    uint32_t len = static_cast<uint32_t>(key.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(key.begin(), key.size());

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
        });
}


void Part::asyncMultiRemove(std::vector<std::string> keys, KVCallback cb) {
    size_t totalLen = 0;
    for (auto& k : keys) {
        totalLen += (sizeof(uint32_t) + k.size());
    }

    std::string log;
    log.reserve(totalLen + sizeof(uint32_t) + 1);

    log.append(&OP_MULTI_REMOVE, 1);
    uint32_t numKeys = keys.size();
    log.append(reinterpret_cast<char*>(&numKeys), sizeof(uint32_t));
    for (auto& k : keys) {
        uint32_t len = k.size();
        log.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
        log.append(k.data(), len);
    }

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
        });
}


void Part::asyncRemovePrefix(folly::StringPiece prefix, KVCallback cb) {
    std::string log;
    log.reserve(prefix.size() + 5);
    log.append(&OP_REMOVE_PREFIX, 1);
    uint32_t len = static_cast<uint32_t>(prefix.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(prefix.begin(), prefix.size());

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
        });
}


void Part::asyncRemoveRange(folly::StringPiece start,
                            folly::StringPiece end,
                            KVCallback cb) {
    std::string log;
    log.reserve(start.size() + end.size() + 9);
    log.append(&OP_REMOVE_RANGE, 1);
    uint32_t len = static_cast<uint32_t>(start.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(start.begin(), start.size());
    len = static_cast<uint32_t>(end.size());
    log.append(reinterpret_cast<char*>(&len), sizeof(len));
    log.append(end.begin(), end.size());

    appendAsync(FLAGS_cluster_id, std::move(log))
        .then([callback = std::move(cb)] (AppendLogResult res) {
            if (res == AppendLogResult::SUCCEEDED) {
                callback(ResultCode::SUCCEEDED);
            } else if (res == AppendLogResult::E_NOT_A_LEADER) {
                callback(ResultCode::ERR_LEADER_CHANGED);
            } else {
                callback(ResultCode::ERR_CONSENSUS_ERROR);
            }
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
    while (iter->valid()) {
        auto log = iter->logMsg();
        switch (log[0]) {
        case OP_PUT: {
            if (batch->putFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::put()";
                return false;
            }
            break;
        }
        case OP_MULTI_PUT: {
            if (batch->multiPutFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::multiPut()";
                return false;
            }
            break;
        }
        case OP_REMOVE: {
            if (batch->removeFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::remove()";
                return false;
            }
            break;
        }
        case OP_MULTI_REMOVE: {
            if (batch->multiRemoveFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::multiRemove()";
                return false;
            }
            break;
        }
        case OP_REMOVE_PREFIX: {
            if (batch->removePrefixFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::removePrefix()";
                return false;
            }
            break;
        }
        case OP_REMOVE_RANGE: {
            if (batch->removeRangeFromLog(log.subpiece(1)) != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failed to call WriteBatch::removeRange()";
                return false;
            }
            break;
        }
        default: {
            LOG(ERROR) << "Unknown operation: " << static_cast<uint8_t>(log[0]);
            return false;
        }
        }

        ++(*iter);
    }

    return engine_->commitBatchWrite(std::move(batch)) == ResultCode::SUCCEEDED;
}

}  // namespace kvstore
}  // namespace nebula

