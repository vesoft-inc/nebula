/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/SnapShot.h"

DEFINE_int32(checkpoint_delay_secs, 30 , "wal synchronization delay");

namespace nebula {
namespace meta {
using SignType = storage::cpp2::EngineSignType;

void CreateSnapshotProcessor::process(const cpp2::CreateSnapshotReq& req) {
    UNUSED(req);
    auto snapshot = currentSnapshot();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());

    auto signRet = Snapshot::instance(kvstore_)->sendBlockSign(SignType::BLOCK_ON);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        onFinished();
        return;
    }

    kvstore_->setBlocking(true);

    // All engines need a delay for wal.
    sleep(FLAGS_checkpoint_delay_secs);

    auto ret = Snapshot::instance(kvstore_)->createSnapshot(snapshot);
    if (ret == cpp2::ErrorCode::SUCCEEDED) {
        nebula::kvstore::ResultCode code = kvstore_->createCheckpoint(kDefaultSpaceId, snapshot);
        if (code != nebula::kvstore::ResultCode::SUCCEEDED) {
            kvstore_->dropCheckpoint(kDefaultSpaceId, snapshot);
            resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        }
    } else {
        Snapshot::instance(kvstore_)->dropSnapshot(snapshot);
        resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    }
    kvstore_->setBlocking(false);
    signRet = Snapshot::instance(kvstore_)->sendBlockSign(SignType::BLOCK_OFF);
    if (signRet != cpp2::ErrorCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    }

    onFinished();
}

std::string CreateSnapshotProcessor::currentSnapshot() {
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    return folly::stringPrintf("SNAPSHOT_%s", ch);
}

}  // namespace meta
}  // namespace nebula

