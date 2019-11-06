/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/SnapShot.h"

namespace nebula {
namespace meta {
using SignType = storage::cpp2::EngineSignType;

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq& req) {
    UNUSED(req);
    auto snapshot = genSnapshotName();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());
    bool successful = true, rollbackMeta = false, rollbackStorage = false, rollbackDone = true;

    // step 1 : Blocking all writes action for storage engines.
    auto signRet = Snapshot::instance(kvstore_)->blockingWrites(SignType::BLOCK_ON);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        resp_.set_code(signRet);
        onFinished();
        return;
    }

    // step 2 : Let recode the snapshot to meta , default snapshot status is CREATING.
    //          The purpose of this is to handle the failure of the checkpoint.
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                      MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::CREATING));

    if (!doSyncPut(std::move(data))) {
        LOG(ERROR) << "Write snapshot meta error";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    // step 3 : Blocking all writes action for meta engine.
    auto ret = kvstore_->setPartBlocking(kDefaultSpaceId, kDefaultPartId, true);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        successful = false;
    }

    // step 4 : Create checkpoint for all storage engines.
    if (successful) {
        auto csRet = Snapshot::instance(kvstore_)->createSnapshot(snapshot);
        if (csRet != cpp2::ErrorCode::SUCCEEDED) {
            successful = false;
            LOG(ERROR) << "Checkpoint create error on storage engine";
            resp_.set_code(csRet);
            rollbackStorage = true;
        }
    }

    // step 5 :
    // Create checkpoint for meta server if all checkpoint created on all storage engines.
    if (successful) {
        // TODO sky : need sync the meta checkpoint to slave hosts.
        auto code = kvstore_->createCheckpoint(kDefaultSpaceId, snapshot);
        if (code != nebula::kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Checkpoint create error on meta engine";
            resp_.set_code(to(code));
            rollbackMeta = rollbackStorage = true;
            successful = false;
        }
    }

    // step 6 : Unblock all engines whatever checkpoint success or failure
    auto reSignRet = Snapshot::instance(kvstore_)->blockingWrites(SignType::BLOCK_OFF);
    if (reSignRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Send blocking sign to storage engine error";
        resp_.set_code(signRet);
        rollbackMeta = rollbackStorage = true;
        successful = false;
    }
    auto code = kvstore_->setPartBlocking(kDefaultSpaceId, kDefaultPartId, false);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        rollbackMeta = rollbackStorage = true;
        successful = false;
    }

    // step 7 : Update meta or rollback checkpoint
    if (successful) {
        // update snapshot status from INVALID to VALID.
        data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                          MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::VALID));

        if (!doSyncPut(std::move(data))) {
            LOG(ERROR) << "All checkpoint create done, but update checkpoint status error. "
                          "snapshot : " << snapshot;
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        }
    } else {
        // Rollback all checkpoints
        if (rollbackStorage) {
            auto dsRet = Snapshot::instance(kvstore_)->dropSnapshot(snapshot);
            if (dsRet != cpp2::ErrorCode::SUCCEEDED) {
                rollbackDone = false;
                LOG(ERROR) << "Checkpoint clear error on storage engine";
                resp_.set_code(dsRet);
            }
        }
        if (rollbackMeta) {
            rollbackDone = false;
            auto dmRet = kvstore_->dropCheckpoint(kDefaultSpaceId, snapshot);
            // TODO sky : need remove meta checkpoint from slave hosts.
            if (dmRet != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Checkpoint clear error on meta engine";
                resp_.set_code(to(dmRet));
            }
        }
        // Clear meta data of checkpoint.
        if (rollbackDone) {
            doRemove(MetaServiceUtils::snapshotKey(snapshot));
            return;
        } else {  // Need to keep metadata if rollback fails
            data.emplace_back(MetaServiceUtils::snapshotKey(snapshot),
                              MetaServiceUtils::snapshotVal(cpp2::SnapshotStatus::INVALID));
            if (!doSyncPut(std::move(data))) {
                LOG(ERROR) << "Update snapshot status error. "
                              "snapshot : " << snapshot;
                resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            }
        }
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

