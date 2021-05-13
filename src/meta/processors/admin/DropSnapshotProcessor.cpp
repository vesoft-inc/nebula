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

    // Check snapshot is exists
    auto key = MetaServiceUtils::snapshotKey(snapshot);
    auto ret = doGet(std::move(key));
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "Get snapshot " << snapshot << " failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto val = nebula::value(ret);

    auto hosts = MetaServiceUtils::parseSnapshotHosts(val);
    auto peersRet = NetworkUtils::toHosts(hosts);
    if (!peersRet.ok()) {
        LOG(ERROR) << "Get checkpoint hosts error";
        handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        onFinished();
        return;
    }


    std::vector<kvstore::KV> data;
    auto peers = peersRet.value();
    auto dsRet = Snapshot::instance(kvstore_, client_)->dropSnapshot(snapshot, std::move(peers));
    if (dsRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Drop snapshot error on storage engine";
        // Need update the snapshot status to invalid, maybe some storage engine drop done.
        data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                          MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
        auto putRet = doSyncPut(std::move(data));
        if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Update snapshot status error. "
                          "snapshot : " << snapshot;
        }
        handleErrorCode(putRet);
        onFinished();
        return;
    }

    auto dmRet = kvstore_->dropCheckpoint(kDefaultSpaceId, snapshot);
    // TODO sky : need remove meta checkpoint from slave hosts.
    if (dmRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Drop snapshot error on meta engine";
        // Need update the snapshot status to invalid, maybe storage engines drop done.
        data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                          MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID, hosts));
        auto putRet = doSyncPut(std::move(data));
        if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Update snapshot status error. "
                          "snapshot : " << snapshot;
        }
        handleErrorCode(putRet);
        onFinished();
        return;
    }
    // Delete metadata of checkpoint
    doRemove(MetaServiceUtils::snapshotKey(snapshot));
    LOG(INFO) << "Drop snapshot " << snapshot << " successfully";
}

}  // namespace meta
}  // namespace nebula
