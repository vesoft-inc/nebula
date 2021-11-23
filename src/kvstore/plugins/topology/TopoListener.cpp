/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/plugins/topology/TopoListener.h"

#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

static const char* kLastCommitId = "_last_commit_id_";
static const char* kLastCommitTerm = "_last_commit_term_";
static const char* kLastApplyId = "_last_apply_id_";

void TopoListener::init() {
}

bool TopoListener::apply(const std::vector<KV>& data) {
  auto batch = engine_->startBatchWrite();
  // we don't write tag/edge value to topo listener, and all data is write to same partition
  for (const auto& kv : data) {
    if (nebula::NebulaKeyUtils::isTag(vIdLen_, kv.first)) {
      auto code = batch->put(NebulaKeyUtils::updatePartIdTagKey(kTopoPartId, kv.first), "");
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << idStr_ << "Failed to write batch";
        return false;
      }
    } else if (nebula::NebulaKeyUtils::isEdge(vIdLen_, kv.first)) {
      auto code = batch->put(NebulaKeyUtils::updatePartIdEdgeKey(kTopoPartId, kv.first), "");
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << idStr_ << "Failed to write batch";
        return false;
      }
    } else {
      continue;
    }
  }
  return engine_->commitBatchWrite(
             std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true) ==
         nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool TopoListener::persist(LogID lastId, TermID lastTerm, LogID applyLogId) {
  auto batch = engine_->startBatchWrite();
  std::string lastCommitId(reinterpret_cast<const char*>(&lastId), sizeof(LogID));
  std::string lastCommitTerm(reinterpret_cast<const char*>(&lastTerm), sizeof(TermID));
  std::string lastApplyId(reinterpret_cast<const char*>(&applyLogId), sizeof(LogID));
  batch->put(kLastCommitId, lastCommitId);
  batch->put(kLastCommitTerm, lastCommitTerm);
  batch->put(kLastApplyId, lastApplyId);
  return engine_->commitBatchWrite(
             std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true) ==
         nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::pair<LogID, TermID> TopoListener::lastCommittedLogId() {
  std::vector<std::string> vals;
  auto result = engine_->multiGet({kLastCommitId, kLastCommitTerm}, &vals);
  for (const auto& status : result) {
    if (!status.ok()) {
      LOG(ERROR) << "Failed to get lastCommitId or lastCommitTerm";
      return {0, 0};
    }
  }
  return {*reinterpret_cast<const LogID*>(vals[0].data()),
          *reinterpret_cast<const TermID*>(vals[1].data())};
}

LogID TopoListener::lastApplyLogId() {
  std::string val;
  auto code = engine_->get(kLastApplyId, &val);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return 0;
  }
  return *reinterpret_cast<const LogID*>(val.data());
}

}  // namespace kvstore
}  // namespace nebula
