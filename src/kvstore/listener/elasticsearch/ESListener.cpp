/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/elasticsearch/ESListener.h"

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "common/utils/NebulaKeyUtils.h"

DECLARE_uint32(ft_request_retry_times);
DECLARE_int32(ft_bulk_batch_size);
DEFINE_int32(listener_commit_batch_size, 1000, "Max batch size when listener commit");

namespace nebula {
namespace kvstore {
void ESListener::init() {
  auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
  if (!vRet.ok()) {
    LOG(FATAL) << "vid length error";
  }
  vIdLen_ = vRet.value();
  auto vidTypeRet = schemaMan_->getSpaceVidType(spaceId_);
  if (!vidTypeRet.ok()) {
    LOG(FATAL) << "vid type error:" << vidTypeRet.status().message();
  }
  isIntVid_ = vidTypeRet.value() == nebula::cpp2::PropertyType::INT64;

  auto sRet = schemaMan_->toGraphSpaceName(spaceId_);
  if (!sRet.ok()) {
    LOG(FATAL) << "space name error";
  }
  spaceName_ = std::make_unique<std::string>(sRet.value());
}

bool ESListener::apply(const BatchHolder& batch) {
  nebula::plugin::ESBulk bulk;
  auto callback = [&bulk](BatchLogType type,
                          const std::string& index,
                          const std::string& vid,
                          const std::string& src,
                          const std::string& dst,
                          int64_t rank,
                          std::map<std::string, std::string> data) {
    if (type == BatchLogType::OP_BATCH_PUT) {
      bulk.put(index, vid, src, dst, rank, std::move(data));
    } else if (type == BatchLogType::OP_BATCH_REMOVE) {
      bulk.delete_(index, vid, src, dst, rank);
    } else {
      LOG(FATAL) << "Unexpect";
    }
  };
  for (const auto& log : batch.getBatch()) {
    pickTagAndEdgeData(std::get<0>(log), std::get<1>(log), std::get<2>(log), callback);
  }
  if (!bulk.empty()) {
    auto esAdapterRes = getESAdapter();
    if (!esAdapterRes.ok()) {
      LOG(ERROR) << esAdapterRes.status();
      return false;
    }
    auto esAdapter = std::move(esAdapterRes).value();
    auto status = esAdapter.bulk(bulk);
    if (!status.ok()) {
      LOG(ERROR) << status;
      return false;
    }
  }
  return true;
}

void ESListener::pickTagAndEdgeData(BatchLogType type,
                                    const std::string& key,
                                    const std::string& value,
                                    const PickFunc& callback) {
  bool isTag = nebula::NebulaKeyUtils::isTag(vIdLen_, key);
  bool isEdge = nebula::NebulaKeyUtils::isEdge(vIdLen_, key);
  if (!(isTag || isEdge)) {
    return;
  }
  std::unordered_map<std::string, nebula::meta::cpp2::FTIndex> ftIndexes;
  nebula::RowReaderWrapper reader;

  std::string vid;
  std::string src;
  std::string dst;
  int rank = 0;
  if (nebula::NebulaKeyUtils::isTag(vIdLen_, key)) {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);
    auto ftIndexRes = schemaMan_->getFTIndex(spaceId_, tagId);
    if (!ftIndexRes.ok()) {
      LOG(ERROR) << ftIndexRes.status().message();
      return;
    }
    ftIndexes = std::move(ftIndexRes).value();
    if (type == BatchLogType::OP_BATCH_PUT) {
      reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, value);
      if (reader == nullptr) {
        LOG(ERROR) << "get tag reader failed, tagID " << tagId;
        return;
      }
    }
    vid = NebulaKeyUtils::getVertexId(vIdLen_, key).toString();
    vid = truncateVid(vid);
  } else {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
    if (edgeType < 0) {
      return;
    }
    auto ftIndexRes = schemaMan_->getFTIndex(spaceId_, edgeType);
    if (!ftIndexRes.ok()) {
      return;
    }
    ftIndexes = std::move(ftIndexRes).value();
    if (type == BatchLogType::OP_BATCH_PUT) {
      reader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, edgeType, value);
      if (reader == nullptr) {
        LOG(ERROR) << "get edge reader failed, schema ID " << edgeType;
        return;
      }
    }
    src = NebulaKeyUtils::getSrcId(vIdLen_, key).toString();
    dst = NebulaKeyUtils::getDstId(vIdLen_, key).toString();
    rank = NebulaKeyUtils::getRank(vIdLen_, key);

    src = truncateVid(src);
    dst = truncateVid(dst);
  }
  if (ftIndexes.empty()) {
    return;
  }

  for (auto& index : ftIndexes) {
    std::map<std::string, std::string> data;
    std::string indexName = index.first;
    if (type == BatchLogType::OP_BATCH_PUT) {
      for (auto& field : index.second.get_fields()) {
        auto v = reader->getValueByName(field);
        if (v.type() == Value::Type::NULLVALUE) {
          data[field] = "";
          continue;
        }
        if (v.type() != Value::Type::STRING) {
          data[field] = "";
          LOG(ERROR) << "Can't create fulltext index on type " << v.type();
          continue;
        }
        data[field] = std::move(v).getStr();
      }
    }
    callback(type, indexName, vid, src, dst, rank, std::move(data));
  }
}

bool ESListener::persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
  if (!writeAppliedId(lastId, lastTerm, lastApplyLogId)) {
    LOG(FATAL) << "last apply ids write failed";
  }
  return true;
}

std::pair<LogID, TermID> ESListener::lastCommittedLogId() {
  if (access(lastApplyLogFile_->c_str(), 0) != 0) {
    VLOG(3) << "Invalid or nonexistent file : " << *lastApplyLogFile_;
    return {0, 0};
  }
  int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" (" << errno
               << "): " << strerror(errno);
  }
  // read last logId from listener wal file.
  LogID logId;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), 0),
           static_cast<ssize_t>(sizeof(LogID)));

  // read last termId from listener wal file.
  TermID termId;
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&termId), sizeof(TermID), sizeof(LogID)),
           static_cast<ssize_t>(sizeof(TermID)));
  close(fd);
  return {logId, termId};
}

LogID ESListener::lastApplyLogId() {
  if (access(lastApplyLogFile_->c_str(), 0) != 0) {
    VLOG(3) << "Invalid or nonexistent file : " << *lastApplyLogFile_;
    return 0;
  }
  int32_t fd = open(lastApplyLogFile_->c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Failed to open the file \"" << lastApplyLogFile_->c_str() << "\" (" << errno
               << "): " << strerror(errno);
  }
  // read last applied logId from listener wal file.
  LogID logId;
  auto offset = sizeof(LogID) + sizeof(TermID);
  CHECK_EQ(pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), offset),
           static_cast<ssize_t>(sizeof(LogID)));
  close(fd);
  return logId;
}

bool ESListener::writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) {
  int32_t fd = open(lastApplyLogFile_->c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    VLOG(3) << "Failed to open file \"" << lastApplyLogFile_->c_str() << "\" (errno: " << errno
            << "): " << strerror(errno);
    return false;
  }
  auto raw = encodeAppliedId(lastId, lastTerm, lastApplyLogId);
  ssize_t written = write(fd, raw.c_str(), raw.size());
  if (written != (ssize_t)raw.size()) {
    VLOG(4) << idStr_ << "bytesWritten:" << written << ", expected:" << raw.size()
            << ", error:" << strerror(errno);
    close(fd);
    return false;
  }
  close(fd);
  return true;
}

std::string ESListener::encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const {
  std::string val;
  val.reserve(sizeof(LogID) * 2 + sizeof(TermID));
  val.append(reinterpret_cast<const char*>(&lastId), sizeof(LogID))
      .append(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID))
      .append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));
  return val;
}

void ESListener::processLogs() {
  std::unique_ptr<LogIterator> iter;
  {
    std::lock_guard<std::mutex> guard(raftLock_);
    if (lastApplyLogId_ >= committedLogId_) {
      return;
    }
    iter = wal_->iterator(lastApplyLogId_ + 1, committedLogId_);
  }

  LogID lastApplyId = -1;
  BatchHolder batch;
  while (iter->valid()) {
    lastApplyId = iter->logId();
    auto log = iter->logMsg();
    if (log.empty()) {
      // skip the heartbeat
      ++(*iter);
      continue;
    }

    DCHECK_GE(log.size(), sizeof(int64_t) + 1 + sizeof(uint32_t));
    switch (log[sizeof(int64_t)]) {
      case OP_PUT: {
        auto pieces = decodeMultiValues(log);
        DCHECK_EQ(2, pieces.size());
        batch.put(pieces[0].toString(), pieces[1].toString());
        break;
      }
      case OP_MULTI_PUT: {
        auto kvs = decodeMultiValues(log);
        DCHECK_EQ(0, kvs.size() % 2);
        for (size_t i = 0; i < kvs.size(); i += 2) {
          batch.put(kvs[i].toString(), kvs[i + 1].toString());
        }
        break;
      }
      case OP_REMOVE: {
        auto key = decodeSingleValue(log);
        batch.remove(key.toString());
        break;
      }
      case OP_REMOVE_RANGE: {
        LOG(WARNING) << "ESListener don't deal with OP_REMOVE_RANGE";
        break;
      }
      case OP_MULTI_REMOVE: {
        auto keys = decodeMultiValues(log);
        for (auto key : keys) {
          batch.remove(key.toString());
        }
        break;
      }
      case OP_BATCH_WRITE: {
        auto batchData = decodeBatchValue(log);
        for (auto& op : batchData) {
          // OP_BATCH_REMOVE and OP_BATCH_REMOVE_RANGE is ignored
          switch (op.first) {
            case BatchLogType::OP_BATCH_PUT: {
              batch.put(op.second.first.toString(), op.second.second.toString());
              break;
            }
            case BatchLogType::OP_BATCH_REMOVE: {
              batch.remove(op.second.first.toString());
              break;
            }
            case BatchLogType::OP_BATCH_REMOVE_RANGE: {
              batch.rangeRemove(op.second.first.toString(), op.second.second.toString());
              break;
            }
          }
        }
        break;
      }
      case OP_TRANS_LEADER:
      case OP_ADD_LEARNER:
      case OP_ADD_PEER:
      case OP_REMOVE_PEER: {
        break;
      }
      default: {
        VLOG(2) << idStr_ << "Unknown operation: " << static_cast<int32_t>(log[0]);
      }
    }

    if (static_cast<int32_t>(batch.size()) > FLAGS_listener_commit_batch_size) {
      break;
    }
    ++(*iter);
  }

  // apply to state machine
  if (lastApplyId != -1 && apply(batch)) {
    std::lock_guard<std::mutex> guard(raftLock_);
    lastApplyLogId_ = lastApplyId;
    persist(committedLogId_, term_, lastApplyLogId_);
    VLOG(2) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
  }
}

std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> ESListener::commitSnapshot(
    const std::vector<std::string>& rows,
    LogID committedLogId,
    TermID committedLogTerm,
    bool finished) {
  VLOG(2) << idStr_ << "Listener is committing snapshot.";
  int64_t count = 0;
  int64_t size = 0;
  BatchHolder batch;
  for (const auto& row : rows) {
    count++;
    size += row.size();
    auto kv = decodeKV(row);
    batch.put(kv.first.toString(), kv.second.toString());
  }
  if (!apply(batch)) {
    LOG(INFO) << idStr_ << "Failed to apply data while committing snapshot.";
    return {
        nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED, kNoSnapshotCount, kNoSnapshotSize};
  }
  if (finished) {
    CHECK(!raftLock_.try_lock());
    leaderCommitId_ = committedLogId;
    lastApplyLogId_ = committedLogId;
    persist(committedLogId, committedLogTerm, lastApplyLogId_);
    LOG(INFO) << folly::sformat(
        "Commit snapshot to : committedLogId={},"
        "committedLogTerm={}, lastApplyLogId_={}",
        committedLogId,
        committedLogTerm,
        lastApplyLogId_);
  }
  return {nebula::cpp2::ErrorCode::SUCCEEDED, count, size};
}

std::string ESListener::truncateVid(const std::string& vid) {
  if (!isIntVid_) {
    return folly::rtrim(folly::StringPiece(vid), [](char c) { return c == '\0'; }).toString();
  }
  return vid;
}

StatusOr<::nebula::plugin::ESAdapter> ESListener::getESAdapter() {
  auto cRet = schemaMan_->getServiceClients(meta::cpp2::ExternalServiceType::ELASTICSEARCH);
  if (!cRet.ok()) {
    LOG(ERROR) << cRet.status().message();
    return cRet.status();
  }
  if (cRet.value().empty()) {
    LOG(ERROR) << "There is no elasticsearch service";
    return ::nebula::Status::Error("There is no elasticsearch service");
  }
  std::vector<nebula::plugin::ESClient> esClients;
  for (const auto& c : cRet.value()) {
    auto host = c.host;
    std::string user, password;
    if (c.user_ref().has_value()) {
      user = *c.user_ref();
      password = *c.pwd_ref();
    }
    std::string protocol = c.conn_type_ref().has_value() ? *c.get_conn_type() : "http";
    esClients.emplace_back(HttpClient::instance(), protocol, host.toRawString(), user, password);
  }
  return ::nebula::plugin::ESAdapter(std::move(esClients));
}

}  // namespace kvstore
}  // namespace nebula
