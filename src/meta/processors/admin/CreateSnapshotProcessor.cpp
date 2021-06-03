/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/SnapShot.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/jobMan/JobManager.h"

namespace nebula {
namespace meta {

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq&) {
    // check the index rebuild. not allowed to create snapshot when index rebuilding.
    JobManager* jobMgr = JobManager::getInstance();
    auto result = jobMgr->checkIndexJobRuning();
    if (!nebula::ok(result)) {
        handleErrorCode(nebula::error(result));
        onFinished();
        return;
    }

    if (nebula::value(result)) {
        LOG(ERROR) << "Index is rebuilding, not allowed to create snapshot.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        onFinished();
        return;
    }

    auto snapshot = folly::format("SNAPSHOT_{}", MetaServiceUtils::genTimestampStr()).str();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
        handleErrorCode(nebula::error(activeHostsRet));
        onFinished();
        return;
    }
    auto hosts = std::move(nebula::value(activeHostsRet));

    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        handleErrorCode(nebula::cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    // step 1 : Let recode the snapshot to meta , default snapshot status is CREATING.
    //          The purpose of this is to handle the failure of the checkpoint.
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID,
                                                    NetworkUtils::toHostsStr(hosts)));

    auto putRet = doSyncPut(std::move(data));
    if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Write snapshot meta error";
        handleErrorCode(putRet);
        onFinished();
        return;
    }

    // step 2 : Blocking all writes action for storage engines.
    auto signRet = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_ON);
    if (signRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        handleErrorCode(signRet);
        cancelWriteBlocking();
        onFinished();
        return;
    }

    // step 3 : Create checkpoint for all storage engines and meta engine.
    auto csRet = Snapshot::instance(kvstore_,  client_)->createSnapshot(snapshot);
    if (!nebula::ok(csRet)) {
        LOG(ERROR) << "Checkpoint create error on storage engine";
        handleErrorCode(nebula::error(csRet));
        cancelWriteBlocking();
        onFinished();
        return;
    }

    // step 4 : checkpoint created done, so release the write blocking.
    auto unbRet = cancelWriteBlocking();
    if (unbRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Create snapshot failed on meta server" << snapshot;
        handleErrorCode(unbRet);
        onFinished();
        return;
    }

    // step 5 : create checkpoint for meta server.
    auto meteRet = kvstore_->createCheckpoint(kDefaultSpaceId, snapshot);
    if (meteRet.isLeftType()) {
        LOG(ERROR) << "Create snapshot failed on meta server" << snapshot;
        handleErrorCode(nebula::cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    // step 6 : update snapshot status from INVALID to VALID.
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::VALID,
                                                    NetworkUtils::toHostsStr(hosts)));

    putRet = doSyncPut(std::move(data));
    if (putRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "All checkpoint creations are done, "
                      "but update checkpoint status error. "
                      "snapshot : " << snapshot;
        handleErrorCode(putRet);
    }

    LOG(INFO) << "Create snapshot " << snapshot << " successfully";
    onFinished();
}

nebula::cpp2::ErrorCode CreateSnapshotProcessor::cancelWriteBlocking() {
    auto signRet = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (signRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Cancel write blocking error";
        return signRet;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

