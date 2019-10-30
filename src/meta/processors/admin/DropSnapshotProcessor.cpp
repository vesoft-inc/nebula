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

    auto paths = kvstore_->getCheckpointPath();
    if (paths.empty()) {
        LOG(ERROR) << "No snapshots found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    if (!fs::FileUtils::exist(paths[0])) {
        resp_.set_code({});
        onFinished();
        return;
    }

    auto dirs = fs::FileUtils::listAllDirsInDir(paths[0].data());

    if (std::find(dirs.begin(), dirs.end(), snapshot) == dirs.end()) {
        LOG(ERROR) << "Snapshot " << snapshot << " does not exist";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto ret = Snapshot::instance(kvstore_)->dropSnapshot(snapshot);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
        onFinished();
        return;
    }
    onFinished();
}
}  // namespace meta
}  // namespace nebula

