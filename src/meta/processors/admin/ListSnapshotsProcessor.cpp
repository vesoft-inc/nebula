/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include <common/fs/FileUtils.h>
#include "meta/processors/admin/ListSnapshotsProcessor.h"

namespace nebula {
namespace meta {

void ListSnapshotsProcessor::process(const cpp2::ListSnapshotsReq&) {
    auto prefix = MetaServiceUtils::snapshotPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }
    decltype(resp_.snapshots) snapshots;
    while (iter->valid()) {
        auto name = MetaServiceUtils::parseSnapshotName(iter->key());
        auto status = MetaServiceUtils::parseSnapshotStatus(iter->val());
        auto hosts = MetaServiceUtils::parseSnapshotHosts(iter->val());
        cpp2::Snapshot snapshot;
        snapshot.set_name(std::move(name));
        snapshot.set_status(std::move(status));
        snapshot.set_hosts(std::move(hosts));
        snapshots.emplace_back(std::move(snapshot));
        iter->next();
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_snapshots(std::move(snapshots));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
