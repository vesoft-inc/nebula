/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/plugins/elasticsearch/ESListener.h"

#include "common/plugin/fulltext/elasticsearch/ESStorageAdapter.h"
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

  auto cRet = schemaMan_->getServiceClients(meta::cpp2::ExternalServiceType::ELASTICSEARCH);
  if (!cRet.ok() || cRet.value().empty()) {
    LOG(FATAL) << "elasticsearch clients error";
  }
  for (const auto& c : cRet.value()) {
    nebula::plugin::HttpClient hc;
    hc.host = c.host;
    if (c.user_ref().has_value()) {
      hc.user = *c.user_ref();
      hc.password = *c.pwd_ref();
    }
    hc.connType = c.conn_type_ref().has_value() ? *c.get_conn_type() : "http";
    esClients_.emplace_back(std::move(hc));
  }

  auto sRet = schemaMan_->toGraphSpaceName(spaceId_);
  if (!sRet.ok()) {
    LOG(FATAL) << "space name error";
  }
  spaceName_ = std::make_unique<std::string>(sRet.value());
}

bool ESListener::apply(const std::vector<KV>& data) {
  std::vector<nebula::plugin::DocItem> docItems;
  for (const auto& kv : data) {
    if (!nebula::NebulaKeyUtils::isTag(vIdLen_, kv.first) &&
        !nebula::NebulaKeyUtils::isEdge(vIdLen_, kv.first)) {
      continue;
    }
    if (!appendDocItem(docItems, kv)) {
      return false;
    }
    if (docItems.size() >= static_cast<size_t>(FLAGS_ft_bulk_batch_size)) {
      auto suc = writeData(docItems);
      if (!suc) {
        return suc;
      }
      docItems.clear();
    }
  }
  if (!docItems.empty()) {
    return writeData(docItems);
  }
  return true;
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

std::string ESListener::encodeAppliedId(LogID lastId,
                                        TermID lastTerm,
                                        LogID lastApplyLogId) const noexcept {
  std::string val;
  val.reserve(sizeof(LogID) * 2 + sizeof(TermID));
  val.append(reinterpret_cast<const char*>(&lastId), sizeof(LogID))
      .append(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID))
      .append(reinterpret_cast<const char*>(&lastApplyLogId), sizeof(LogID));
  return val;
}

bool ESListener::appendDocItem(std::vector<DocItem>& items, const KV& kv) const {
  auto isEdge = NebulaKeyUtils::isEdge(vIdLen_, kv.first);
  return isEdge ? appendEdgeDocItem(items, kv) : appendTagDocItem(items, kv);
}

bool ESListener::appendEdgeDocItem(std::vector<DocItem>& items, const KV& kv) const {
  auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, kv.first);
  auto ftIndex = schemaMan_->getFTIndex(spaceId_, edgeType);
  if (!ftIndex.ok()) {
    VLOG(3) << "get text search index failed";
    return (ftIndex.status() == nebula::Status::IndexNotFound()) ? true : false;
  }
  auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, edgeType, kv.second);
  if (reader == nullptr) {
    VLOG(3) << "get edge reader failed, schema ID " << edgeType;
    return false;
  }
  return appendDocs(items, reader.get(), std::move(ftIndex).value());
}

bool ESListener::appendTagDocItem(std::vector<DocItem>& items, const KV& kv) const {
  auto tagId = NebulaKeyUtils::getTagId(vIdLen_, kv.first);
  auto ftIndex = schemaMan_->getFTIndex(spaceId_, tagId);
  if (!ftIndex.ok()) {
    VLOG(3) << "get text search index failed";
    return (ftIndex.status() == nebula::Status::IndexNotFound()) ? true : false;
  }
  auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, kv.second);
  if (reader == nullptr) {
    VLOG(3) << "get tag reader failed, tagID " << tagId;
    return false;
  }
  return appendDocs(items, reader.get(), std::move(ftIndex).value());
}

bool ESListener::appendDocs(std::vector<DocItem>& items,
                            RowReader* reader,
                            const std::pair<std::string, nebula::meta::cpp2::FTIndex>& fti) const {
  for (const auto& field : fti.second.get_fields()) {
    auto v = reader->getValueByName(field);
    if (v.type() != Value::Type::STRING) {
      continue;
    }
    items.emplace_back(DocItem(fti.first, field, partId_, std::move(v).getStr()));
  }
  return true;
}

bool ESListener::writeData(const std::vector<nebula::plugin::DocItem>& items) const {
  bool isNeedWriteOneByOne = false;
  auto retryCnt = FLAGS_ft_request_retry_times;
  while (--retryCnt > 0) {
    auto index = folly::Random::rand32(esClients_.size() - 1);
    auto suc = nebula::plugin::ESStorageAdapter::kAdapter->bulk(esClients_[index], items);
    if (!suc.ok()) {
      VLOG(3) << "bulk failed. retry : " << retryCnt;
      continue;
    }
    if (!suc.value()) {
      isNeedWriteOneByOne = true;
      break;
    }
    return true;
  }
  if (isNeedWriteOneByOne) {
    return writeDatum(items);
  }
  LOG(WARNING) << idStr_ << "Failed to bulk into es.";
  return false;
}

bool ESListener::writeDatum(const std::vector<nebula::plugin::DocItem>& items) const {
  bool done = false;
  for (const auto& item : items) {
    done = false;
    auto retryCnt = FLAGS_ft_request_retry_times;
    while (--retryCnt > 0) {
      auto index = folly::Random::rand32(esClients_.size() - 1);
      auto suc = nebula::plugin::ESStorageAdapter::kAdapter->put(esClients_[index], item);
      if (!suc.ok()) {
        VLOG(3) << "put failed. retry : " << retryCnt;
        continue;
      }
      if (!suc.value()) {
        // TODO (sky) : Record failed data
        break;
      }
      done = true;
      break;
    }
    if (!done) {
      // means CURL fails, and no need to take the next step
      LOG(INFO) << idStr_ << "Failed to put into es.";
      return false;
    }
  }
  return true;
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
  // the kv pair which can sync to remote safely
  std::vector<KV> data;
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
        data.emplace_back(pieces[0], pieces[1]);
        break;
      }
      case OP_MULTI_PUT: {
        auto kvs = decodeMultiValues(log);
        DCHECK_EQ(0, kvs.size() % 2);
        for (size_t i = 0; i < kvs.size(); i += 2) {
          data.emplace_back(kvs[i], kvs[i + 1]);
        }
        break;
      }
      case OP_REMOVE:
      case OP_REMOVE_RANGE:
      case OP_MULTI_REMOVE: {
        break;
      }
      case OP_BATCH_WRITE: {
        auto batch = decodeBatchValue(log);
        for (auto& op : batch) {
          // OP_BATCH_REMOVE and OP_BATCH_REMOVE_RANGE is igored
          if (op.first == BatchLogType::OP_BATCH_PUT) {
            data.emplace_back(op.second.first, op.second.second);
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

    if (static_cast<int32_t>(data.size()) > FLAGS_listener_commit_batch_size) {
      break;
    }
    ++(*iter);
  }
  // apply to state machine
  if (lastApplyId != -1 && apply(data)) {
    std::lock_guard<std::mutex> guard(raftLock_);
    lastApplyLogId_ = lastApplyId;
    persist(committedLogId_, term_, lastApplyLogId_);
    VLOG(2) << idStr_ << "Listener succeeded apply log to " << lastApplyLogId_;
    lastApplyTime_ = time::WallClock::fastNowInMilliSec();
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
  std::vector<KV> data;
  data.reserve(rows.size());
  for (const auto& row : rows) {
    count++;
    size += row.size();
    auto kv = decodeKV(row);
    data.emplace_back(kv.first, kv.second);
  }
  if (!apply(data)) {
    LOG(INFO) << idStr_ << "Failed to apply data while committing snapshot.";
    return {
        nebula::cpp2::ErrorCode::E_RAFT_PERSIST_SNAPSHOT_FAILED, kNoSnapshotCount, kNoSnapshotSize};
  }
  if (finished) {
    CHECK(!raftLock_.try_lock());
    leaderCommitId_ = committedLogId;
    lastApplyLogId_ = committedLogId;
    persist(committedLogId, committedLogTerm, lastApplyLogId_);
    lastApplyTime_ = time::WallClock::fastNowInMilliSec();
    LOG(INFO) << folly::sformat(
        "Commit snapshot to : committedLogId={},"
        "committedLogTerm={}, lastApplyLogId_={}",
        committedLogId,
        committedLogTerm,
        lastApplyLogId_);
  }
  return {nebula::cpp2::ErrorCode::SUCCEEDED, count, size};
}

}  // namespace kvstore
}  // namespace nebula
