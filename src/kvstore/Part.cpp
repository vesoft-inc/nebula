/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/Part.h"

#include "common/time/ScopedTimer.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "common/utils/Utils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/RocksEngineConfig.h"
#include "kvstore/stats/KVStats.h"

DEFINE_int32(cluster_id, 0, "A unique id for each cluster");

namespace nebula {
namespace kvstore {

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
           std::shared_ptr<DiskManager> diskMan,
           int32_t vIdLen)
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
               diskMan),
      spaceId_(spaceId),
      partId_(partId),
      walPath_(walPath),
      engine_(engine),
      vIdLen_(vIdLen) {}

std::pair<LogID, TermID> Part::lastCommittedLogId() {
  std::string val;
  auto res = engine_->get(NebulaKeyUtils::systemCommitKey(partId_), &val);
  if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << idStr_ << "Cannot fetch the last committed log id from the storage engine";
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
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncAppendBatch(std::string&& batch, KVCallback cb) {
  appendAsync(FLAGS_cluster_id, std::move(batch))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncMultiPut(const std::vector<KV>& keyValues, KVCallback cb) {
  std::string log = encodeMultiValues(OP_MULTI_PUT, keyValues);

  appendAsync(FLAGS_cluster_id, std::move(log))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncRemove(folly::StringPiece key, KVCallback cb) {
  std::string log = encodeSingleValue(OP_REMOVE, key);

  appendAsync(FLAGS_cluster_id, std::move(log))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncMultiRemove(const std::vector<std::string>& keys, KVCallback cb) {
  std::string log = encodeMultiValues(OP_MULTI_REMOVE, keys);

  appendAsync(FLAGS_cluster_id, std::move(log))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncRemoveRange(folly::StringPiece start, folly::StringPiece end, KVCallback cb) {
  std::string log = encodeMultiValues(OP_REMOVE_RANGE, start, end);

  appendAsync(FLAGS_cluster_id, std::move(log))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::sync(KVCallback cb) {
  sendCommandAsync("").thenValue(
      [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncAtomicOp(MergeableAtomicOp op, KVCallback cb) {
  atomicOpAsync(std::move(op))
      .thenValue(
          [callback = std::move(cb)](nebula::cpp2::ErrorCode code) mutable { callback(code); });
}

void Part::asyncAddLearner(const HostAddr& learner, KVCallback cb) {
  std::string log = encodeHost(OP_ADD_LEARNER, learner);
  sendCommandAsync(std::move(log))
      .thenValue([callback = std::move(cb), learner, this](nebula::cpp2::ErrorCode code) mutable {
        VLOG(1) << idStr_ << "add learner " << learner
                << ", result: " << apache::thrift::util::enumNameSafe(code);
        callback(code);
      });
}

void Part::asyncTransferLeader(const HostAddr& target, KVCallback cb) {
  std::string log = encodeHost(OP_TRANS_LEADER, target);
  sendCommandAsync(std::move(log))
      .thenValue([callback = std::move(cb), target, this](nebula::cpp2::ErrorCode code) mutable {
        VLOG(1) << idStr_ << "transfer leader to " << target
                << ", result: " << apache::thrift::util::enumNameSafe(code);
        callback(code);
      });
}

void Part::asyncAddPeer(const HostAddr& peer, KVCallback cb) {
  std::string log = encodeHost(OP_ADD_PEER, peer);
  sendCommandAsync(std::move(log))
      .thenValue([callback = std::move(cb), peer, this](nebula::cpp2::ErrorCode code) mutable {
        VLOG(1) << idStr_ << "add peer " << peer
                << ", result: " << apache::thrift::util::enumNameSafe(code);
        callback(code);
      });
}

void Part::asyncRemovePeer(const HostAddr& peer, KVCallback cb) {
  std::string log = encodeHost(OP_REMOVE_PEER, peer);
  sendCommandAsync(std::move(log))
      .thenValue([callback = std::move(cb), peer, this](nebula::cpp2::ErrorCode code) mutable {
        VLOG(1) << idStr_ << "remove peer " << peer
                << ", result: " << apache::thrift::util::enumNameSafe(code);
        callback(code);
      });
}

void Part::setBlocking(bool sign) {
  blocking_ = sign;
}

void Part::onLostLeadership(TermID term) {
  VLOG(2) << "Lost the leadership for the term " << term;

  CallbackOptions opt;
  opt.spaceId = spaceId_;
  opt.partId = partId_;
  opt.term = term_;

  for (auto& cb : leaderLostCB_) {
    cb(opt);
  }
}

void Part::onElected(TermID term) {
  VLOG(2) << "Being elected as the leader for the term: " << term;
}

void Part::onLeaderReady(TermID term) {
  VLOG(2) << "leader ready to server for the term: " << term;

  CallbackOptions opt;
  opt.spaceId = spaceId_;
  opt.partId = partId_;
  opt.term = term_;

  for (auto& cb : leaderReadyCB_) {
    cb(opt);
  }
}

void Part::registerOnLeaderReady(LeaderChangeCB cb) {
  leaderReadyCB_.emplace_back(std::move(cb));
}

void Part::registerOnLeaderLost(LeaderChangeCB cb) {
  leaderLostCB_.emplace_back(std::move(cb));
}

void Part::onDiscoverNewLeader(HostAddr nLeader) {
  VLOG(2) << idStr_ << "Find the new leader " << nLeader;
  if (newLeaderCb_) {
    newLeaderCb_(nLeader);
  }
}

std::tuple<nebula::cpp2::ErrorCode, LogID, TermID> Part::commitLogs(
    std::unique_ptr<LogIterator> iter, bool wait, bool needLock) {
  // We should apply any membership change which happens before start time. Because when we start
  // up, the peers comes from meta, has already contains all previous changes.
  SCOPED_TIMER([](uint64_t elapsedTime) {
    stats::StatsManager::addValue(kCommitLogLatencyUs, elapsedTime);
  });
  auto batch = engine_->startBatchWrite();
  LogID lastId = kNoCommitLogId;
  TermID lastTerm = kNoCommitLogTerm;
  while (iter->valid()) {
    lastId = iter->logId();
    lastTerm = iter->logTerm();
    auto log = iter->logMsg();
    if (log.empty()) {
      VLOG(4) << idStr_ << "Skip the heartbeat!";
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
          VLOG(3) << idStr_ << "Failed to call WriteBatch::put()";
          return {code, kNoCommitLogId, kNoCommitLogTerm};
        }
        break;
      }
      case OP_MULTI_PUT: {
        auto kvs = decodeMultiValues(log);
        // Make the number of values are an even number
        DCHECK_EQ((kvs.size() + 1) / 2, kvs.size() / 2);
        for (size_t i = 0; i < kvs.size(); i += 2) {
          VLOG(4) << "OP_MULTI_PUT " << folly::hexlify(kvs[i])
                  << ", val = " << folly::hexlify(kvs[i + 1]);
          auto code = batch->put(kvs[i], kvs[i + 1]);
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(3) << idStr_ << "Failed to call WriteBatch::put()";
            return {code, kNoCommitLogId, kNoCommitLogTerm};
          }
        }
        break;
      }
      case OP_REMOVE: {
        auto key = decodeSingleValue(log);
        auto code = batch->remove(key);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          VLOG(3) << idStr_ << "Failed to call WriteBatch::remove()";
          return {code, kNoCommitLogId, kNoCommitLogTerm};
        }
        break;
      }
      case OP_MULTI_REMOVE: {
        auto keys = decodeMultiValues(log);
        for (auto k : keys) {
          auto code = batch->remove(k);
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(3) << idStr_ << "Failed to call WriteBatch::remove()";
            return {code, kNoCommitLogId, kNoCommitLogTerm};
          }
        }
        break;
      }
      case OP_REMOVE_RANGE: {
        auto range = decodeMultiValues(log);
        DCHECK_EQ(2, range.size());
        auto code = batch->removeRange(range[0], range[1]);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
          VLOG(3) << idStr_ << "Failed to call WriteBatch::removeRange()";
          return {code, kNoCommitLogId, kNoCommitLogTerm};
        }
        break;
      }
      case OP_BATCH_WRITE: {
        auto data = decodeBatchValue(log);
        for (auto& op : data) {
          VLOG(4) << "OP_BATCH_WRITE: " << folly::hexlify(op.second.first)
                  << ", val = " << folly::hexlify(op.second.second);
          auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
          if (op.first == BatchLogType::OP_BATCH_PUT) {
            code = batch->put(op.second.first, op.second.second);
          } else if (op.first == BatchLogType::OP_BATCH_REMOVE) {
            code = batch->remove(op.second.first);
          } else if (op.first == BatchLogType::OP_BATCH_REMOVE_RANGE) {
            code = batch->removeRange(op.second.first, op.second.second);
          }
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(3) << idStr_ << "Failed to call WriteBatch";
            return {code, kNoCommitLogId, kNoCommitLogTerm};
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
          commitTransLeader(newLeader, needLock);
        } else {
          VLOG(2) << idStr_ << "Skip commit stale transfer leader " << newLeader
                  << ", the part is opened at " << startTimeMs_ << ", but the log timestamp is "
                  << ts;
        }
        break;
      }
      case OP_REMOVE_PEER: {
        auto peer = decodeHost(OP_REMOVE_PEER, log);
        auto ts = getTimestamp(log);
        if (ts > startTimeMs_) {
          commitRemovePeer(peer, needLock);
        } else {
          VLOG(2) << idStr_ << "Skip commit stale remove peer " << peer
                  << ", the part is opened at " << startTimeMs_ << ", but the log timestamp is "
                  << ts;
        }
        break;
      }
      default: {
        VLOG(3) << idStr_
                << "Should not reach here. Unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }

    ++(*iter);
  }

  if (lastId >= 0) {
    auto code = putCommitMsg(batch.get(), lastId, lastTerm);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "Put commit id into batch failed";
      return {code, kNoCommitLogId, kNoCommitLogTerm};
    }
  }

  auto code = engine_->commitBatchWrite(
      std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, wait);
  if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
    return {code, lastId, lastTerm};
  } else {
    return {code, kNoCommitLogId, kNoCommitLogTerm};
  }
}

std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> Part::commitSnapshot(
    const std::vector<std::string>& rows,
    LogID committedLogId,
    TermID committedLogTerm,
    bool finished) {
  SCOPED_TIMER([](uint64_t elapsedTime) {
    stats::StatsManager::addValue(kCommitSnapshotLatencyUs, elapsedTime);
  });
  auto batch = engine_->startBatchWrite();
  int64_t count = 0;
  int64_t size = 0;
  for (auto& row : rows) {
    count++;
    size += row.size();
    auto kv = decodeKV(row);
    auto code = batch->put(kv.first, kv.second);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "Failed to call WriteBatch::put()";
      return {code, kNoSnapshotCount, kNoSnapshotSize};
    }
  }
  if (finished) {
    auto code = putCommitMsg(batch.get(), committedLogId, committedLogTerm);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      VLOG(3) << idStr_ << "Put commit id into batch failed";
      return {code, kNoSnapshotCount, kNoSnapshotSize};
    }
  }
  // For snapshot, we open the rocksdb's wal to avoid loss data if crash.
  auto code = engine_->commitBatchWrite(
      std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return {code, kNoSnapshotCount, kNoSnapshotSize};
  }
  return {code, count, size};
}

nebula::cpp2::ErrorCode Part::putCommitMsg(WriteBatch* batch,
                                           LogID committedLogId,
                                           TermID committedLogTerm) {
  std::string commitMsg;
  commitMsg.reserve(sizeof(LogID) + sizeof(TermID));
  commitMsg.append(reinterpret_cast<char*>(&committedLogId), sizeof(LogID));
  commitMsg.append(reinterpret_cast<char*>(&committedLogTerm), sizeof(TermID));
  return batch->put(NebulaKeyUtils::systemCommitKey(partId_), commitMsg);
}

bool Part::preProcessLog(LogID logId, TermID termId, ClusterID clusterId, folly::StringPiece log) {
  // We should apply any membership change which happens before start time. Because when we start
  // up, the peers comes from meta, has already contains all previous changes.
  VLOG(4) << idStr_ << "logId " << logId << ", termId " << termId << ", clusterId " << clusterId;
  if (!log.empty()) {
    switch (log[sizeof(int64_t)]) {
      case OP_ADD_LEARNER: {
        auto learner = decodeHost(OP_ADD_LEARNER, log);
        auto ts = getTimestamp(log);
        if (ts > startTimeMs_) {
          VLOG(1) << idStr_ << "preprocess add learner " << learner;
          addLearner(learner, false);
          // persist the part learner info in case of storaged restarting
          auto ret = engine_->updatePart(partId_, Peer(learner, Peer::Status::kLearner));
          if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return false;
          }
        } else {
          VLOG(1) << idStr_ << "Skip stale add learner " << learner << ", the part is opened at "
                  << startTimeMs_ << ", but the log timestamp is " << ts;
        }
        break;
      }
      case OP_TRANS_LEADER: {
        auto newLeader = decodeHost(OP_TRANS_LEADER, log);
        auto ts = getTimestamp(log);
        if (ts > startTimeMs_) {
          VLOG(1) << idStr_ << "preprocess trans leader " << newLeader;
          preProcessTransLeader(newLeader);
        } else {
          VLOG(1) << idStr_ << "Skip stale transfer leader " << newLeader
                  << ", the part is opened at " << startTimeMs_ << ", but the log timestamp is "
                  << ts;
        }
        break;
      }
      case OP_ADD_PEER: {
        auto peer = decodeHost(OP_ADD_PEER, log);
        auto ts = getTimestamp(log);
        if (ts > startTimeMs_) {
          VLOG(1) << idStr_ << "preprocess add peer " << peer;
          addPeer(peer);
          auto ret = engine_->updatePart(partId_, Peer(peer, Peer::Status::kPromotedPeer));
          if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return false;
          }
        } else {
          VLOG(1) << idStr_ << "Skip stale add peer " << peer << ", the part is opened at "
                  << startTimeMs_ << ", but the log timestamp is " << ts;
        }
        break;
      }
      case OP_REMOVE_PEER: {
        auto peer = decodeHost(OP_REMOVE_PEER, log);
        auto ts = getTimestamp(log);
        if (ts > startTimeMs_) {
          VLOG(1) << idStr_ << "preprocess remove peer " << peer;
          preProcessRemovePeer(peer);
          // remove peer in the persist info
          auto ret = engine_->updatePart(partId_, Peer(peer, Peer::Status::kDeleted));
          if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return false;
          }
        } else {
          VLOG(1) << idStr_ << "Skip stale remove peer " << peer << ", the part is opened at "
                  << startTimeMs_ << ", but the log timestamp is " << ts;
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

nebula::cpp2::ErrorCode Part::cleanup() {
  if (spaceId_ == kDefaultSpaceId && partId_ == kDefaultPartId) {
    return metaCleanup();
  }
  LOG(INFO) << idStr_ << "Clean rocksdb part data";
  auto batch = engine_->startBatchWrite();
  // Remove the vertex, edge, index, systemCommitKey, operation data under the part

  const auto& kvPre = NebulaKeyUtils::kvPrefix(partId_);
  auto ret =
      batch->removeRange(NebulaKeyUtils::firstKey(kvPre, 128), NebulaKeyUtils::lastKey(kvPre, 128));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup kvdata, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  const auto& tagPre = NebulaKeyUtils::tagPrefix(partId_);
  ret = batch->removeRange(NebulaKeyUtils::firstKey(tagPre, vIdLen_),
                           NebulaKeyUtils::lastKey(tagPre, vIdLen_));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup tag, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  const auto& edgePre = NebulaKeyUtils::edgePrefix(partId_);
  ret = batch->removeRange(NebulaKeyUtils::firstKey(edgePre, vIdLen_),
                           NebulaKeyUtils::lastKey(edgePre, vIdLen_));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup edge, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  const auto& indexPre = IndexKeyUtils::indexPrefix(partId_);
  ret = batch->removeRange(NebulaKeyUtils::firstKey(indexPre, sizeof(IndexID)),
                           NebulaKeyUtils::lastKey(indexPre, sizeof(IndexID)));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup index, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  const auto& operationPre = OperationKeyUtils::operationPrefix(partId_);
  ret = batch->removeRange(NebulaKeyUtils::firstKey(operationPre, sizeof(int64_t)),
                           NebulaKeyUtils::lastKey(operationPre, sizeof(int64_t)));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup operation, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  const auto& vertexPre = NebulaKeyUtils::vertexPrefix(partId_);
  ret = batch->removeRange(NebulaKeyUtils::firstKey(vertexPre, vIdLen_),
                           NebulaKeyUtils::lastKey(vertexPre, vIdLen_));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Failed to encode removeRange() when cleanup operation, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  // todo(doodle): toss prime and double prime

  ret = batch->remove(NebulaKeyUtils::systemCommitKey(partId_));
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(3) << idStr_ << "Remove the part system commit data failed, error "
            << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }
  return engine_->commitBatchWrite(
      std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true);
}

nebula::cpp2::ErrorCode Part::metaCleanup() {
  std::string kMetaPrefix = "__";
  auto firstKey = NebulaKeyUtils::firstKey(kMetaPrefix, 1);
  auto lastKey = NebulaKeyUtils::lastKey(kMetaPrefix, 1);
  // todo(doodle): since the poor performance of DeleteRange, perhaps we need to compact
  return engine_->removeRange(firstKey, lastKey);
}

}  // namespace kvstore
}  // namespace nebula
