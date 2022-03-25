/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/CreateBackupProcessor.h"

#include "common/time/TimeUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/SnapShot.h"
#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

ErrorOr<nebula::cpp2::ErrorCode, std::unordered_set<GraphSpaceID>>
CreateBackupProcessor::spaceNameToId(const std::vector<std::string>* backupSpaces) {
  std::unordered_set<GraphSpaceID> spaces;

  bool allSpaces = backupSpaces == nullptr || backupSpaces->empty();
  if (!allSpaces) {
    std::vector<std::string> keys;
    keys.reserve(backupSpaces->size());
    std::transform(
        backupSpaces->begin(), backupSpaces->end(), std::back_inserter(keys), [](auto& name) {
          return MetaKeyUtils::indexSpaceKey(name);
        });

    auto result = doMultiGet(std::move(keys));
    if (!nebula::ok(result)) {
      auto err = nebula::error(result);
      LOG(INFO) << "Failed to get space id, error: " << apache::thrift::util::enumNameSafe(err);
      if (err == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        return nebula::cpp2::ErrorCode::E_BACKUP_SPACE_NOT_FOUND;
      }
      return err;
    }

    auto values = std::move(nebula::value(result));
    std::transform(std::make_move_iterator(values.begin()),
                   std::make_move_iterator(values.end()),
                   std::inserter(spaces, spaces.end()),
                   [](const std::string& rawID) {
                     return *reinterpret_cast<const GraphSpaceID*>(rawID.c_str());
                   });

  } else {
    const auto& prefix = MetaKeyUtils::spacePrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
      auto retCode = nebula::error(iterRet);
      LOG(INFO) << "Space prefix failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
      auto spaceId = MetaKeyUtils::spaceId(iter->key());
      auto spaceName = MetaKeyUtils::spaceName(iter->val());
      VLOG(3) << "List spaces " << spaceId << ", name " << spaceName;
      spaces.emplace(spaceId);
      iter->next();
    }
  }

  if (spaces.empty()) {
    LOG(INFO) << "Failed to create a full backup because there is currently "
                 "no space.";
    return nebula::cpp2::ErrorCode::E_BACKUP_SPACE_NOT_FOUND;
  }

  return spaces;
}

void CreateBackupProcessor::process(const cpp2::CreateBackupReq& req) {
  auto* backupSpaces = req.get_spaces();
  auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
  if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_LEADER_CHANGED);
    onFinished();
    return;
  }
  JobManager* jobMgr = JobManager::getInstance();

  // make sure there is no index job
  std::unordered_set<cpp2::JobType> jobTypes{cpp2::JobType::REBUILD_TAG_INDEX,
                                             cpp2::JobType::REBUILD_EDGE_INDEX};
  auto result = jobMgr->checkTypeJobRunning(jobTypes);
  if (!nebula::ok(result)) {
    LOG(INFO) << "Get Index status failed, not allowed to create backup.";
    handleErrorCode(nebula::error(result));
    onFinished();
    return;
  }
  if (nebula::value(result)) {
    LOG(INFO) << "Index is rebuilding, not allowed to create backup.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_BACKUP_BUILDING_INDEX);
    onFinished();
    return;
  }

  folly::SharedMutex::WriteHolder holder(LockUtils::snapshotLock());
  // get active storage host list
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }
  auto hosts = std::move(nebula::value(activeHostsRet));
  if (hosts.empty()) {
    LOG(INFO) << "There are some offline hosts";
    handleErrorCode(nebula::cpp2::ErrorCode::E_NO_HOSTS);
    onFinished();
    return;
  }

  // transform space names to id list
  auto spaceIdRet = spaceNameToId(backupSpaces);
  if (!nebula::ok(spaceIdRet)) {
    handleErrorCode(nebula::error(spaceIdRet));
    onFinished();
    return;
  }
  auto spaces = nebula::value(spaceIdRet);

  // The entire process follows mostly snapshot logic.
  // step 1 : write a flag key to handle backup failed
  std::vector<kvstore::KV> data;
  auto backupName = folly::sformat("BACKUP_{}", MetaKeyUtils::genTimestampStr());
  data.emplace_back(
      MetaKeyUtils::snapshotKey(backupName),
      MetaKeyUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, NetworkUtils::toHostsStr(hosts)));
  auto putRet = doSyncPut(data);
  if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Write backup meta error";
    handleErrorCode(putRet);
    onFinished();
    return;
  }

  Snapshot::instance(kvstore_, client_)->setSpaces(spaces);
  // step 2 : Blocking all writes action for storage engines.
  auto ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_ON);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Send blocking sign to storage engine error";
    handleErrorCode(ret);
    ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Cancel write blocking error:" << apache::thrift::util::enumNameSafe(ret);
    }
    onFinished();
    return;
  }

  // step 3 : Create checkpoint for all storage engines.
  auto sret = Snapshot::instance(kvstore_, client_)->createSnapshot(backupName);
  if (!nebula::ok(sret)) {
    LOG(INFO) << "Checkpoint create error on storage engine: "
              << apache::thrift::util::enumNameSafe(nebula::error(sret));
    handleErrorCode(nebula::error(sret));
    ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Cancel write blocking error:" << apache::thrift::util::enumNameSafe(ret);
    }
    onFinished();
    return;
  }

  // step 4 created backup for meta(export sst).
  auto backupFiles = MetaServiceUtils::backupTables(kvstore_, spaces, backupName, backupSpaces);
  if (!nebula::ok(backupFiles)) {
    LOG(INFO) << "Failed backup meta";
    handleErrorCode(nebula::cpp2::ErrorCode::E_BACKUP_FAILED);
    ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Cancel write blocking error:" << apache::thrift::util::enumNameSafe(ret);
    }
    onFinished();
    return;
  }

  // step 5 : checkpoint created done, so release the write blocking.
  ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Cancel write blocking error";
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // step 6 : update backup status from INVALID to VALID.
  data.clear();
  data.emplace_back(
      MetaKeyUtils::snapshotKey(backupName),
      MetaKeyUtils::snapshotVal(cpp2::SnapshotStatus::VALID, NetworkUtils::toHostsStr(hosts)));
  putRet = doSyncPut(std::move(data));
  if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "All checkpoint creations are done, "
                 "but update checkpoint status error. "
                 "backup : "
              << backupName;
    handleErrorCode(putRet);
    onFinished();
    return;
  }

  // space backup info:
  //  - space desc
  //  - backup info list:
  //    - backup info:
  //      - host
  //      - checkpoint info:
  //        - space id
  //        - parts
  //        - path
  std::unordered_map<GraphSpaceID, cpp2::SpaceBackupInfo> backups;
  auto snapshotInfo = std::move(nebula::value(sret));
  for (auto id : spaces) {
    LOG(INFO) << "backup space " << id;
    cpp2::SpaceBackupInfo spaceBackup;
    auto spaceKey = MetaKeyUtils::spaceKey(id);
    auto spaceRet = doGet(spaceKey);
    if (!nebula::ok(spaceRet)) {
      handleErrorCode(nebula::error(spaceRet));
      onFinished();
      return;
    }
    auto spaceDesc = MetaKeyUtils::parseSpace(nebula::value(spaceRet));
    // todo we should save partition info.
    auto it = snapshotInfo.find(id);
    DCHECK(it != snapshotInfo.end());

    spaceBackup.host_backups_ref() = std::move(it->second);
    spaceBackup.space_ref() = std::move(spaceDesc);
    backups.emplace(id, std::move(spaceBackup));
  }

  cpp2::BackupMeta backup;
  LOG(INFO) << "sst files count was:" << nebula::value(backupFiles).size();
  backup.meta_files_ref() = std::move(nebula::value(backupFiles));
  backup.space_backups_ref() = std::move(backups);
  backup.backup_name_ref() = std::move(backupName);
  backup.full_ref() = true;
  bool allSpaces = backupSpaces == nullptr || backupSpaces->empty();
  backup.all_spaces_ref() = allSpaces;
  backup.create_time_ref() = time::WallClock::fastNowInMilliSec();

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.meta_ref() = std::move(backup);
  LOG(INFO) << "backup done";

  onFinished();
}

}  // namespace meta
}  // namespace nebula
