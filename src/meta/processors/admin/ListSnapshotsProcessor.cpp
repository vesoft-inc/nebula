/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <common/fs/FileUtils.h>
#include "meta/processors/admin/ListSnapshotsProcessor.h"

namespace nebula {
namespace meta {

void ListSnapshotsProcessor::process(const cpp2::ListSnapshotsReq& req) {
    UNUSED(req);
    auto paths = kvstore_->getCheckpointPath();
    if (paths.empty()) {
        LOG(ERROR) << "checkpoint path error";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    // Because all paths contain the same checkpoint structure, so only the first one is needed.
    if (!fs::FileUtils::exist(paths[0])) {
        resp_.set_code({});
        onFinished();
        return;
    }

    auto dirs = fs::FileUtils::listAllDirsInDir(paths[0].data());
    decltype(resp_.snapshots) snapshots;
    for (auto& dir : dirs) {
        snapshots.emplace_back(dir);
    }
    resp_.set_snapshots(std::move(snapshots));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
