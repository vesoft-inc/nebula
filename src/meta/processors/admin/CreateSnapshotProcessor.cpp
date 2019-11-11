/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/SnapShot.h"
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {
using SignType = storage::cpp2::EngineSignType;

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq& req) {
    UNUSED(req);
    auto snapshot = genSnapshotName();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());

    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    // step 1 : Let recode the snapshot to meta , default snapshot status is CREATING.
    //          The purpose of this is to handle the failure of the checkpoint.
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID,
                                                    NetworkUtils::toHosts(hosts)));

    if (!doSyncPut(std::move(data))) {
        LOG(ERROR) << "Write snapshot meta error";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        clusterCheck_->addUnblockingTask();
        return;
    }

    // step 2 : Blocking all writes action for storage engines.
    auto signRet = Snapshot::instance(kvstore_)->blockingWrites(SignType::BLOCK_ON);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        resp_.set_code(signRet);
        onFinished();
        clusterCheck_->addUnblockingTask();
        return;
    }

    // step 4 : Create checkpoint for all storage engines and meta engine.
    auto csRet = Snapshot::instance(kvstore_)->createSnapshot(snapshot);
    if (csRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Checkpoint create error on storage engine";
        resp_.set_code(csRet);
        onFinished();
        clusterCheck_->addUnblockingTask();
        clusterCheck_->addDropCheckpointTask(snapshot);
        return;
    }

    // Execute unblocking immediately
    signRet = Snapshot::instance(kvstore_)->blockingWrites(SignType::BLOCK_OFF);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "All checkpoint create done, but unblocking error. "
                      "so the status of the snapshot cannot be change to valid. "
                      "snapshot : " << snapshot;
        resp_.set_code(signRet);
        onFinished();
        clusterCheck_->addUnblockingTask();
        return;
    }

    // step 7 : update snapshot status from INVALID to VALID.
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::VALID,
                                                    NetworkUtils::toHosts(hosts)));

    if (!doSyncPut(std::move(data))) {
        LOG(ERROR) << "All checkpoint create done, but update checkpoint status error. "
                      "snapshot : " << snapshot;
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
    }

    onFinished();
}

std::string CreateSnapshotProcessor::genSnapshotName() {
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    return folly::stringPrintf("SNAPSHOT_%s", ch);
}

}  // namespace meta
}  // namespace nebula

