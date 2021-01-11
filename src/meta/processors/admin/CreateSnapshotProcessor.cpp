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

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq&) {
    // check the index rebuild. not allowed to create snapshot when index rebuilding.
    auto prefix = MetaServiceUtils::rebuildIndexStatusPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }
    while (iter->valid()) {
        if (iter->val() == "RUNNING") {
            LOG(ERROR) << "Index is rebuilding, not allowed to block write.";
            handleErrorCode(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
            onFinished();
            return;
        }
        iter->next();
    }
    auto snapshot = genSnapshotName();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());

    auto hosts = ActiveHostsMan::getActiveHosts(kvstore_);
    if (hosts.empty()) {
        LOG(ERROR) << "There is no active hosts";
        handleErrorCode(cpp2::ErrorCode::E_NO_HOSTS);
        onFinished();
        return;
    }

    // step 1 : Let recode the snapshot to meta , default snapshot status is CREATING.
    //          The purpose of this is to handle the failure of the checkpoint.
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID,
                                                    NetworkUtils::toHosts(hosts)));

    auto putRet = doSyncPut(std::move(data));
    if (putRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Write snapshot meta error";
        handleErrorCode(MetaCommon::to(putRet));
        onFinished();
        return;
    }

    // step 2 : Blocking all writes action for storage engines.
    auto signRet = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_ON);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        handleErrorCode(signRet);
        cancelWriteBlocking();
        onFinished();
        return;
    }

    // step 3 : Create checkpoint for all storage engines and meta engine.
    auto csRet = Snapshot::instance(kvstore_,  client_)->createSnapshot(snapshot);
    if (csRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Checkpoint create error on storage engine";
        handleErrorCode(csRet);
        cancelWriteBlocking();
        onFinished();
        return;
    }

    // step 4 : checkpoint created done, so release the write blocking.
    auto unbRet = cancelWriteBlocking();
    if (unbRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Create snapshot failed on meta server" << snapshot;
        handleErrorCode(unbRet);
        onFinished();
        return;
    }

    // step 5 : create checkpoint for meta server.
    auto meteRet = kvstore_->createCheckpoint(kDefaultSpaceId, snapshot);
    if (meteRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Create snapshot failed on meta server" << snapshot;
        handleErrorCode(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    // step 6 : update snapshot status from INVALID to VALID.
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::VALID,
                                                    NetworkUtils::toHosts(hosts)));

    putRet = doSyncPut(std::move(data));
    if (putRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "All checkpoint creations are done, "
                      "but update checkpoint status error. "
                      "snapshot : " << snapshot;
        handleErrorCode(MetaCommon::to(putRet));
    }

    LOG(INFO) << "Create snapshot " << snapshot << " successfully";
    onFinished();
}

std::string CreateSnapshotProcessor::genSnapshotName() {
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    return folly::stringPrintf("SNAPSHOT_%s", ch);
}

cpp2::ErrorCode CreateSnapshotProcessor::cancelWriteBlocking() {
    auto signRet = Snapshot::instance(kvstore_, client_)->blockingWrites(SignType::BLOCK_OFF);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Cancel write blocking error";
        return signRet;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula

