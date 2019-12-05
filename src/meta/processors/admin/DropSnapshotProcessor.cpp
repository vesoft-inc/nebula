/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/DropSnapshotProcessor.h"
#include "meta/processors/admin/SnapShot.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace meta {

void DropSnapshotProcessor::process(const cpp2::DropSnapshotReq& req) {
    auto& snapshot = req.get_name();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());
    std::string val;

    // Check snapshot is exists
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId,
                             MetaServiceUtils::snapshotKey(snapshot),
                             &val);

    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "No snapshots found";
        resp_.set_code(to(ret));
        onFinished();
        return;
    }

    auto hosts = MetaServiceUtils::parseSnapshotHosts(val);
    auto peers = NetworkUtils::toHosts(hosts);
    if (!peers.ok()) {
        LOG(ERROR) << "Get checkpoint hosts error";
        resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    auto dsRet = Snapshot::instance(kvstore_)->dropSnapshot(snapshot, std::move(peers.value()));
    if (dsRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Drop snapshot error on storage engine";
        // Need update the snapshot status to invalid, maybe some storage engine drop done.
        data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                          MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
        if (!doSyncPut(std::move(data))) {
            LOG(ERROR) << "Update snapshot status error. "
                          "snapshot : " << snapshot;
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        }
        onFinished();
        return;
    }

    auto dmRet = kvstore_->dropCheckpoint(kDefaultSpaceId, snapshot);
    // TODO sky : need remove meta checkpoint from slave hosts.
    if (dmRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Drop snapshot error on meta engine";
        // Need update the snapshot status to invalid, maybe storage engines drop done.
        data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                          MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
        if (!doSyncPut(std::move(data))) {
            LOG(ERROR) << "Update snapshot status error. "
                          "snapshot : " << snapshot;
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        } else {
            resp_.set_code(to(dmRet));
        }
        onFinished();
        return;
    }
    // Delete metadata of checkpoint
    doRemove(MetaServiceUtils::snapshotKey(snapshot));
}
}  // namespace meta
}  // namespace nebula

