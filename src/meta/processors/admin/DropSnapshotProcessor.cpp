/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/DropSnapshotProcessor.h"

#include "common/fs/FileUtils.h"
#include "kvstore/LogEncoder.h"
#include "meta/processors/admin/SnapShot.h"

namespace nebula {
namespace meta {

void DropSnapshotProcessor::process(const cpp2::DropSnapshotReq& req) {
  auto& snapshots = req.get_names();
  auto snapshot = snapshots[0];
  folly::SharedMutex::WriteHolder holder(LockUtils::snapshotLock());

  // Check snapshot is exists
  auto key = MetaKeyUtils::snapshotKey(snapshot);
  auto ret = doGet(key);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(INFO) << "Snapshot " << snapshot << " does not exist or already dropped.";
      onFinished();
      return;
    }
    LOG(INFO) << "Get snapshot " << snapshot << " failed, error "
              << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto val = nebula::value(ret);

  auto hosts = MetaKeyUtils::parseSnapshotHosts(val);
  auto peersRet = NetworkUtils::toHosts(hosts);
  if (!peersRet.ok()) {
    LOG(INFO) << "Get checkpoint hosts error";
    handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    onFinished();
    return;
  }

  auto peers = peersRet.value();
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  auto dsRet = Snapshot::instance(kvstore_, client_)->dropSnapshot(snapshot, std::move(peers));
  if (dsRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Drop snapshot error on storage engine";
    // Need update the snapshot status to invalid, maybe some storage engine
    // drop done.
    batchHolder->put(MetaKeyUtils::snapshotKey(snapshot),
                     MetaKeyUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
  }

  auto dmRet = kvstore_->dropCheckpoint(kDefaultSpaceId, snapshot);
  // TODO sky : need remove meta checkpoint from slave hosts.
  if (dmRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Drop snapshot error on meta engine";
    // Need update the snapshot status to invalid, maybe storage engines drop
    // done.
    batchHolder->put(MetaKeyUtils::snapshotKey(snapshot),
                     MetaKeyUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
  }

  // Delete metadata of checkpoint
  batchHolder->remove(std::move(key));
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
  LOG(INFO) << "Drop snapshot " << snapshot << " successfully";
}

}  // namespace meta
}  // namespace nebula
