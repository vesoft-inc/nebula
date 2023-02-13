/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/CreateSnapshotProcessor.h"

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/SnapShot.h"
#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq&) {
  // check the index rebuild. not allowed to create snapshot when index
  // rebuilding.
  JobManager* jobMgr = JobManager::getInstance();
  std::unordered_set<cpp2::JobType> jobTypes{cpp2::JobType::REBUILD_TAG_INDEX,
                                             cpp2::JobType::REBUILD_EDGE_INDEX,
                                             cpp2::JobType::COMPACT,
                                             cpp2::JobType::INGEST,
                                             cpp2::JobType::DATA_BALANCE,
                                             cpp2::JobType::LEADER_BALANCE};
  auto result = jobMgr->checkTypeJobRunning(jobTypes);
  if (!nebula::ok(result)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    onFinished();
    return;
  }
  if (nebula::value(result)) {
    LOG(INFO) << "Mutating data job is running, not allowed to create snapshot.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_RUNNING_JOBS);
    onFinished();
    return;
  }

  auto snapshot = folly::sformat("SNAPSHOT_{}", MetaKeyUtils::genTimestampStr());
  folly::SharedMutex::WriteHolder holder(LockUtils::snapshotLock());

  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }
  auto hosts = std::move(nebula::value(activeHostsRet));
  if (hosts.empty()) {
    LOG(INFO) << "There is no active hosts";
    handleErrorCode(nebula::cpp2::ErrorCode::E_NO_HOSTS);
    onFinished();
    return;
  }

  std::unordered_set<HostAddr> machines;
  auto ret = getAllMachines(machines);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(ret);
    onFinished();
    return;
  }
  // since all active hosts are checked registered, then we only need check count here
  if (hosts.size() != machines.size()) {
    LOG(INFO) << "There are some hosts registered(by `ADD HOSTS`) but not active, please "
                 "ensure all storaged active and you haven't registered useless machine;"
              << "registered machines count=" << machines.size()
              << "; active hosts count=" << hosts.size();
    handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  cpp2::SnapshotStatus status = cpp2::SnapshotStatus::VALID;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    // Step 1 : Blocking all writes action for storage engines.
    ret = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_ON);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Send blocking sign to storage engine error";
      code = ret;
      cancelWriteBlocking();
      status = cpp2::SnapshotStatus::INVALID;
      break;
    }

    // Step 2 : Create checkpoint for all storage engines and meta engine.
    auto csRet = Snapshot::instance(kvstore_, client_)->createSnapshot(snapshot);
    if (!nebula::ok(csRet)) {
      LOG(INFO) << "Checkpoint create error on storage engine";
      code = nebula::error(csRet);
      cancelWriteBlocking();
      status = cpp2::SnapshotStatus::INVALID;
      break;
    }

    // Step 3 : checkpoint created done, so release the write blocking.
    ret = cancelWriteBlocking();
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Create snapshot failed on meta server" << snapshot;
      code = ret;
      status = cpp2::SnapshotStatus::INVALID;
      break;
    }

    // Step 4 : Create checkpoint for meta server.
    auto meteRet = kvstore_->createCheckpoint(kDefaultSpaceId, snapshot);
    if (meteRet.isLeftType()) {
      LOG(INFO) << "Create snapshot failed on meta server" << snapshot;
      code = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
      status = cpp2::SnapshotStatus::INVALID;
      break;
    }
  } while (false);

  // Save the snapshot status.
  data.emplace_back(MetaKeyUtils::snapshotKey(snapshot),
                    MetaKeyUtils::snapshotVal(status, NetworkUtils::toHostsStr(hosts)));

  auto putRet = doSyncPut(std::move(data));
  if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "All checkpoint creations are done, "
              << "but update checkpoint status error. snapshot : " << snapshot;
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      code = putRet;
    }
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Create snapshot " << snapshot
              << " failed: " << apache::thrift::util::enumNameSafe(code);
  } else {
    LOG(INFO) << "Create snapshot " << snapshot << " successfully";
  }
  handleErrorCode(code);
  onFinished();
}

nebula::cpp2::ErrorCode CreateSnapshotProcessor::cancelWriteBlocking() {
  auto signRet = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
  if (signRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Cancel write blocking error";
  }
  return signRet;
}

}  // namespace meta
}  // namespace nebula
